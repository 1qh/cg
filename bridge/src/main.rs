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
use futures::stream::{self, Stream};
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
    let port = std::env::var("PORT").unwrap_or_else(|_| "4054".into());
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
    if has_tools { b = b.with_tool_config(ToolConfig { ..Default::default() }); }
    let mut tc = ThinkingConfig::new().with_thoughts_included(true);
    if let Some(r) = &req.reasoning { if let Some(e) = r.effort {
        let lvl = match e { CodexEffort::Minimal=>ThinkingLevel::Minimal, CodexEffort::Low=>ThinkingLevel::Low, CodexEffort::Medium=>ThinkingLevel::Medium, CodexEffort::High|CodexEffort::Xhigh=>ThinkingLevel::High };
        tc = tc.with_thinking_level(lvl);
    }}
    b = b.with_thinking_config(tc);

    let rid = format!("resp_{}", uuid::Uuid::new_v4().simple());
    let mut seq = 0u64;
    let mk_resp = |status: Status, output: Vec<OutputItem>| Response {
        background: None, billing: None, conversation: None, created_at: 0, completed_at: None,
        error: None, id: rid.clone(), incomplete_details: None, instructions: None, max_output_tokens: None,
        metadata: None, model: model.clone(), object: "response".into(), output, parallel_tool_calls: None,
        previous_response_id: None, prompt: None, prompt_cache_key: None, prompt_cache_retention: None,
        reasoning: None, safety_identifier: None, service_tier: None, status, temperature: None,
        text: None, tool_choice: None, tools: None, top_logprobs: None, top_p: None, truncation: None, usage: None,
    };
    let to_ev = |e: ResponseStreamEvent| -> Result<Event, Infallible> { Ok(Event::default().data(serde_json::to_string(&e).unwrap())) };
    let mut events: Vec<Result<Event, Infallible>> = Vec::new();

    events.push(to_ev(ResponseStreamEvent::ResponseCreated(ResponseCreatedEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::InProgress, vec![]) })));
    events.push(to_ev(ResponseStreamEvent::ResponseInProgress(ResponseInProgressEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::InProgress, vec![]) })));

    let resp = match b.execute().await {
        Ok(r) => r,
        Err(e) => { events.push(to_ev(ResponseStreamEvent::ResponseFailed(ResponseFailedEvent { sequence_number: { seq+=1; seq }, response: mk_resp(Status::Failed, vec![]) }))); let _=e; return Sse::new(stream::iter(events)); }
    };
    let um = resp.usage_metadata.clone();
    let usage = um.map(|u| ResponseUsage { input_tokens: u.prompt_token_count.unwrap_or(0) as u32, input_tokens_details: InputTokenDetails { cached_tokens: u.cached_content_token_count.unwrap_or(0) as u32 }, output_tokens: u.candidates_token_count.unwrap_or(0) as u32, output_tokens_details: OutputTokenDetails { reasoning_tokens: u.thoughts_token_count.unwrap_or(0) as u32 }, total_tokens: u.total_token_count.unwrap_or(0) as u32 });
    // typed emit: reasoning + function_call + message
    let (mut text, mut reasoning, mut rsn_sig) = (String::new(), String::new(), String::new());
    let mut fcs: Vec<(String, String)> = Vec::new();
    if let Some(c) = resp.candidates.into_iter().next() { if let Some(parts) = c.content.parts { for p in parts {
        match p {
            Part::Text { text: t, thought: Some(true), thought_signature } => { reasoning.push_str(&t); if let Some(sg)=thought_signature { rsn_sig=sg; } }
            Part::Text { text: t, .. } => { text.push_str(&t); }
            Part::FunctionCall { function_call, thought_signature } => { if let Some(sg)=thought_signature { rsn_sig=sg; } fcs.push((function_call.name, serde_json::to_string(&function_call.args).unwrap_or_else(|_| "{}".into()))); }
            _ => {}
        }
    }}}
    let mut out_items: Vec<OutputItem> = Vec::new();
    let mut oi = 0u32;
    if !reasoning.is_empty() || !rsn_sig.is_empty() {
        let ri = ReasoningItem { id: Some(format!("rs_{}", uuid::Uuid::new_v4().simple())), summary: vec![SummaryPart::SummaryText(SummaryTextContent { text: reasoning.clone() })], content: None, encrypted_content: Some(rsn_sig.clone()), status: Some(OutputStatus::Completed) };
        events.push(to_ev(ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::Reasoning(ri.clone()) })));
        events.push(to_ev(ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::Reasoning(ri.clone()) })));
        out_items.push(OutputItem::Reasoning(ri)); oi += 1;
    }
    for (name, args) in fcs {
        let fc_id = format!("fc_{}", uuid::Uuid::new_v4().simple());
        let fc = FunctionToolCall { arguments: args.clone(), call_id: format!("call_{}", uuid::Uuid::new_v4().simple()), namespace: None, name, id: Some(fc_id.clone()), status: Some(OutputStatus::Completed) };
        events.push(to_ev(ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::FunctionCall(fc.clone()) })));
        events.push(to_ev(ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(ResponseFunctionCallArgumentsDeltaEvent { sequence_number: { seq+=1; seq }, item_id: fc_id.clone(), output_index: oi, delta: args.clone() })));
        events.push(to_ev(ResponseStreamEvent::ResponseFunctionCallArgumentsDone(ResponseFunctionCallArgumentsDoneEvent { name: None, sequence_number: { seq+=1; seq }, item_id: fc_id, output_index: oi, arguments: args })));
        events.push(to_ev(ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::FunctionCall(fc.clone()) })));
        out_items.push(OutputItem::FunctionCall(fc)); oi += 1;
    }
    if !text.is_empty() {
        let msg_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        let msg = OutputMessage { content: vec![OutputMessageContent::OutputText(OutputTextContent { text: text.clone(), annotations: vec![], logprobs: None })], id: msg_id.clone(), role: AssistantRole::Assistant, phase: None, status: OutputStatus::Completed };
        events.push(to_ev(ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::Message(msg.clone()) })));
        events.push(to_ev(ResponseStreamEvent::ResponseOutputTextDelta(ResponseTextDeltaEvent { sequence_number: { seq+=1; seq }, item_id: msg_id, output_index: oi, content_index: 0, delta: text.clone(), logprobs: None })));
        events.push(to_ev(ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent { sequence_number: { seq+=1; seq }, output_index: oi, item: OutputItem::Message(msg.clone()) })));
        out_items.push(OutputItem::Message(msg));
    }
    let mut done_resp = mk_resp(Status::Completed, out_items); done_resp.usage = usage;
    events.push(to_ev(ResponseStreamEvent::ResponseCompleted(ResponseCompletedEvent { sequence_number: { seq+=1; seq }, response: done_resp })));
    Sse::new(stream::iter(events))
}
