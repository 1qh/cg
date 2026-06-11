//! MAX-TYPESAFE bridge: async-openai CreateResponse (typed request) -> gemini-rust (typed gemini) ->
//! async-openai ResponseStreamEvent (typed emit). No raw serde_json::Value for request/response shapes.
use async_openai::types::responses::{
    OutputItem, OutputMessage, OutputMessageContent,
    Response, ResponseStreamEvent, ResponseCreatedEvent, ResponseInProgressEvent,
    ResponseOutputItemAddedEvent, ResponseOutputItemDoneEvent, ResponseTextDeltaEvent,
    ResponseCompletedEvent, ResponseFailedEvent, AssistantRole, OutputStatus, OutputTextContent,
    Status, FunctionToolCall, ReasoningItem, SummaryPart, SummaryTextContent,
    ResponseFunctionCallArgumentsDeltaEvent, ResponseFunctionCallArgumentsDoneEvent,
};
use async_openai::types::responses::{InputTokenDetails, OutputTokenDetails, ResponseUsage};
use axum::{extract::State, response::sse::{Event, Sse}, routing::{get, post}, Json, Router};
use futures::stream::Stream;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use gemini_rust::{Content, FunctionCall, FunctionDeclaration, FunctionResponse, Gemini, Model, Part, Role, ThinkingConfig, ThinkingLevel, Tool as GTool};
use gemini_rust::tools::ToolConfig;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct CodexReq {
    #[serde(default)] model: Option<String>,
    #[serde(default)] instructions: Option<String>,
    #[serde(default)] input: Vec<CodexInput>,
    #[serde(default)] tools: Vec<CodexTool>,
    #[serde(default)] reasoning: Option<CodexReasoning>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexInput {
    Message { role: CodexRole, #[serde(default)] content: Vec<CodexContent> },
    Reasoning { #[serde(default)] encrypted_content: Option<String> },
    FunctionCall { #[serde(default)] call_id: String, #[serde(default)] name: String, #[serde(default)] arguments: String },
    FunctionCallOutput { #[serde(default)] call_id: String, #[serde(default)] output: String },
    #[serde(other)] Other,
}
#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum CodexRole { User, Assistant, System, Developer, #[serde(other)] Other }
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CodexEffort { Minimal, Low, Medium, High, Xhigh }
#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum CodexToolKind { Function, #[serde(other)] Other }
#[derive(Deserialize)]
struct CodexContent { #[serde(default)] text: Option<String> }
#[derive(Deserialize)]
struct CodexTool { #[serde(rename = "type")] kind: CodexToolKind, #[serde(default)] name: String, #[serde(default)] description: String, #[serde(default)] parameters: Option<Value> }
#[derive(Deserialize)]
struct CodexReasoning { #[serde(default)] effort: Option<CodexEffort> }
use std::convert::Infallible;

#[derive(Clone)]
struct AppState { api_key: String }

#[tokio::main]
async fn main() {
    let api_key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY");
    let app = Router::new().route("/v1/responses", post(responses))
        .route("/health/liveliness", get(|| async { "ok" })).with_state(AppState { api_key });
    let port = std::env::var("PORT").expect("PORT env required (no fallback)");
    let l = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.unwrap();
    eprintln!("typed bridge on :{port}");
    axum::serve(l, app).await.unwrap();
}

async fn responses(State(st): State<AppState>, Json(req): Json<CodexReq>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let model = req.model.clone().unwrap_or_else(|| "gemini-3.5-flash".into());
    // typed input -> gemini-rust contents (faithful + typed)
    let mut names: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for it in &req.input { if let CodexInput::FunctionCall { call_id, name, .. } = it { names.insert(call_id.clone(), name.clone()); } }
    let mut contents: Vec<Content> = Vec::new();
    let mut pending_sig: Option<String> = None;
    for it in &req.input {
        match it {
            CodexInput::Message { role, content } => {
                let txt: String = content.iter().filter_map(|c| c.text.clone()).collect::<Vec<_>>().join("");
                if txt.is_empty() { continue; }
                contents.push(Content::text(txt).with_role(if *role == CodexRole::Assistant { Role::Model } else { Role::User }));
            }
            CodexInput::Reasoning { encrypted_content } => { if let Some(e) = encrypted_content { if !e.is_empty() { pending_sig = Some(e.clone()); } } }
            CodexInput::FunctionCall { name, arguments, .. } => {
                let args: Value = serde_json::from_str(arguments).unwrap_or_default();
                let sig = pending_sig.take().unwrap_or_else(|| "skip_thought_signature_validator".into());
                contents.push(Content::function_call_with_thought(FunctionCall::new(name, args), sig).with_role(Role::Model));
            }
            CodexInput::FunctionCallOutput { call_id, output } => {
                let name = names.get(call_id).cloned().unwrap_or_else(|| "unknown".into());
                contents.push(Content::function_response(FunctionResponse::new(name, serde_json::json!({"output": output}))).with_role(Role::User));
            }
            CodexInput::Other => {}
        }
    }
    let api_model = if model.starts_with("models/") { model.clone() } else { format!("models/{model}") };
    let client = Gemini::with_model(st.api_key.clone(), Model::Custom(api_model)).expect("client");
    let mut b = client.generate_content();
    b.contents = contents;
    if let Some(ins) = &req.instructions { b = b.with_system_prompt(ins.clone()); }
    let mut has_tools = false;
    for t in &req.tools {
        if t.kind != CodexToolKind::Function { continue; }
        has_tools = true;
        let mut p = t.parameters.clone().unwrap_or_else(|| serde_json::json!({"type":"object","properties":{}}));
        fn san(v:&mut Value){match v{Value::Object(m)=>{m.remove("additionalProperties");m.remove("$schema");for(_,x)in m.iter_mut(){san(x);}}Value::Array(a)=>{for x in a.iter_mut(){san(x);}}_=>{}}}
        san(&mut p);
        b = b.with_tool(GTool::new(FunctionDeclaration::new(&t.name, &t.description, None).with_parameters_value(p)));
    }
    // grounding injection: built-in web/url tools on every request (model grounds selectively);
    // includeServerSideToolInvocations unlocks the union (built-in + function tools coexisting).
    let _ = has_tools;
    b = b.with_tool(GTool::google_search());
    b = b.with_tool(GTool::url_context());
    b = b.with_tool_config(ToolConfig { include_server_side_tool_invocations: Some(true), ..Default::default() });
    let mut tc = ThinkingConfig::new().with_thoughts_included(true);
    if let Some(r) = &req.reasoning { if let Some(e) = r.effort {
        let lvl = match e { CodexEffort::Minimal=>ThinkingLevel::Minimal, CodexEffort::Low=>ThinkingLevel::Low, CodexEffort::Medium=>ThinkingLevel::Medium, CodexEffort::High|CodexEffort::Xhigh=>ThinkingLevel::High };
        tc = tc.with_thinking_level(lvl);
    }}
    b = b.with_thinking_config(tc);

    let rid = format!("resp_{}", uuid::Uuid::new_v4().simple());
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(64);
    tokio::spawn(async move {
        let mut seq = 0u64;
        let mk_resp = |status: Status, output: Vec<OutputItem>, usage: Option<ResponseUsage>| Response {
            background: None, billing: None, conversation: None, created_at: 0, completed_at: None,
            error: None, id: rid.clone(), incomplete_details: None, instructions: None, max_output_tokens: None,
            metadata: None, model: model.clone(), object: "response".into(), output, parallel_tool_calls: None,
            previous_response_id: None, prompt: None, prompt_cache_key: None, prompt_cache_retention: None,
            reasoning: None, safety_identifier: None, service_tier: None, status, temperature: None,
            text: None, tool_choice: None, tools: None, top_logprobs: None, top_p: None, truncation: None, usage,
        };
        let to_ev = |e: ResponseStreamEvent| -> Result<Event, Infallible> { Ok(Event::default().data(serde_json::to_string(&e).unwrap())) };
        macro_rules! send { ($e:expr) => { if tx.send($e).await.is_err() { return; } }; }

        send!(to_ev(ResponseStreamEvent::ResponseCreated(ResponseCreatedEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::InProgress, vec![], None) })));
        send!(to_ev(ResponseStreamEvent::ResponseInProgress(ResponseInProgressEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::InProgress, vec![], None) })));

        let mut stream = match b.execute_stream().await {
            Ok(s) => s,
            Err(_) => { send!(to_ev(ResponseStreamEvent::ResponseFailed(ResponseFailedEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::Failed, vec![], None) }))); return; }
        };
        // accumulate reasoning (thought parts arrive before text/tool); emit incremental text deltas live.
        let (mut text, mut reasoning, mut rsn_sig) = (String::new(), String::new(), String::new());
        let mut out_items: Vec<OutputItem> = Vec::new();
        let mut fcs: Vec<(String, String)> = Vec::new();
        let mut usage: Option<ResponseUsage> = None;
        let (mut got_finish, mut finish): (bool, Option<gemini_rust::FinishReason>) = (false, None);
        let mut oi = 0u32;
        let mut rsn_emitted = false;
        let mut msg_open = false;
        let mut msg_id = String::new();
        let mut msg_oi = 0u32;
        // flush the accumulated reasoning item as soon as the first text/tool part arrives (preserves
        // reasoning->message ordering + the real thought_signature round-trip, exactly as non-stream).
        macro_rules! flush_reasoning { () => {
            if !rsn_emitted && (!reasoning.is_empty() || !rsn_sig.is_empty()) {
                rsn_emitted = true;
                let ri = ReasoningItem { id: Some(format!("rs_{}", uuid::Uuid::new_v4().simple())), summary: vec![SummaryPart::SummaryText(SummaryTextContent { text: reasoning.clone() })], content: None, encrypted_content: Some(rsn_sig.clone()), status: Some(OutputStatus::Completed) };
                send!(to_ev(ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::Reasoning(ri.clone()) })));
                send!(to_ev(ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::Reasoning(ri.clone()) })));
                out_items.push(OutputItem::Reasoning(ri)); oi += 1;
            }
        }; }
        while let Some(item) = stream.next().await {
            let chunk = match item { Ok(c) => c, Err(_) => break };
            if let Some(c) = chunk.candidates.into_iter().next() {
                if let Some(parts) = c.content.parts { for p in parts {
                    match p {
                        Part::Text { text: t, thought: Some(true), thought_signature } => { reasoning.push_str(&t); if let Some(sg)=thought_signature { rsn_sig=sg; } }
                        Part::Text { text: t, .. } if !t.is_empty() => {
                            flush_reasoning!();
                            if !msg_open {
                                msg_open = true; msg_oi = oi; oi += 1;
                                msg_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
                                let msg = OutputMessage { content: vec![], id: msg_id.clone(), role: AssistantRole::Assistant, phase: None, status: OutputStatus::InProgress };
                                send!(to_ev(ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent { sequence_number: { seq+=1; seq }, output_index: msg_oi, item: OutputItem::Message(msg) })));
                            }
                            text.push_str(&t);
                            send!(to_ev(ResponseStreamEvent::ResponseOutputTextDelta(ResponseTextDeltaEvent { sequence_number: { seq+=1; seq }, item_id: msg_id.clone(), output_index: msg_oi, content_index: 0, delta: t, logprobs: None })));
                        }
                        Part::FunctionCall { function_call, thought_signature } => { if let Some(sg)=thought_signature { rsn_sig=sg; } flush_reasoning!(); fcs.push((function_call.name, serde_json::to_string(&function_call.args).unwrap_or_else(|_| "{}".into()))); }
                        _ => {}
                    }
                }}
                if let Some(fr) = c.finish_reason { got_finish = true; finish = Some(fr); }
            }
            if let Some(u) = chunk.usage_metadata {
                usage = Some(ResponseUsage { input_tokens: u.prompt_token_count.unwrap_or(0) as u32, input_tokens_details: InputTokenDetails { cached_tokens: u.cached_content_token_count.unwrap_or(0) as u32 }, output_tokens: u.candidates_token_count.unwrap_or(0) as u32, output_tokens_details: OutputTokenDetails { reasoning_tokens: u.thoughts_token_count.unwrap_or(0) as u32 }, total_tokens: u.total_token_count.unwrap_or(0) as u32 });
            }
        }
        flush_reasoning!();
        // close the streamed message item with its final text
        if msg_open {
            let msg = OutputMessage { content: vec![OutputMessageContent::OutputText(OutputTextContent { text: text.clone(), annotations: vec![], logprobs: None })], id: msg_id.clone(), role: AssistantRole::Assistant, phase: None, status: OutputStatus::Completed };
            send!(to_ev(ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent { sequence_number: { seq+=1; seq }, output_index: msg_oi, item: OutputItem::Message(msg.clone()) })));
            // keep output order reasoning -> message -> tools to match the response.output array shape
            out_items.insert(if rsn_emitted { 1.min(out_items.len()) } else { 0 }, OutputItem::Message(msg));
        }
        for (name, args) in fcs {
            let fc_id = format!("fc_{}", uuid::Uuid::new_v4().simple());
            let fc = FunctionToolCall { arguments: args.clone(), call_id: format!("call_{}", uuid::Uuid::new_v4().simple()), namespace: None, name, id: Some(fc_id.clone()), status: Some(OutputStatus::Completed) };
            send!(to_ev(ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::FunctionCall(fc.clone()) })));
            send!(to_ev(ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(ResponseFunctionCallArgumentsDeltaEvent { sequence_number: { seq+=1; seq }, item_id: fc_id.clone(), output_index: oi, delta: args.clone() })));
            send!(to_ev(ResponseStreamEvent::ResponseFunctionCallArgumentsDone(ResponseFunctionCallArgumentsDoneEvent { name: None, sequence_number: { seq+=1; seq }, item_id: fc_id, output_index: oi, arguments: args })));
            send!(to_ev(ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::FunctionCall(fc.clone()) })));
            out_items.push(OutputItem::FunctionCall(fc)); oi += 1;
        }
        use gemini_rust::FinishReason as FR;
        let incomplete = match finish { Some(FR::MaxTokens) => Some("max_output_tokens"), Some(FR::Safety) | Some(FR::Recitation) | Some(FR::ImageSafety) => Some("content_filter"), _ => None };
        if got_finish {
            if let Some(r) = incomplete {
                let mut resp = mk_resp(Status::Incomplete, out_items, usage);
                resp.incomplete_details = Some(async_openai::types::responses::IncompleteDetails { reason: r.into() });
                send!(to_ev(ResponseStreamEvent::ResponseIncomplete(async_openai::types::responses::ResponseIncompleteEvent { sequence_number: { seq+=1; seq }, response: resp })));
            } else {
                send!(to_ev(ResponseStreamEvent::ResponseCompleted(ResponseCompletedEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::Completed, out_items, usage) })));
            }
        } else {
            send!(to_ev(ResponseStreamEvent::ResponseFailed(ResponseFailedEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::Failed, out_items, usage) })));
        }
    });
    Sse::new(ReceiverStream::new(rx))
}
