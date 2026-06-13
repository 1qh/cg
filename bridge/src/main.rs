//! MAX-TYPESAFE bridge: async-openai `CreateResponse` (typed request) -> gemini-rust (typed gemini) ->
//! async-openai `ResponseStreamEvent` (typed emit). No raw `serde_json::Value` for request/response shapes.
use core::convert::Infallible;
use std::io::Write as _;

use async_openai::types::responses::{
    AssistantRole, FunctionToolCall, InputTokenDetails, OutputItem, OutputMessage,
    OutputMessageContent, OutputStatus, OutputTextContent, OutputTokenDetails, ReasoningItem,
    Response, ResponseCompletedEvent, ResponseCreatedEvent, ResponseFailedEvent,
    ResponseFunctionCallArgumentsDeltaEvent, ResponseFunctionCallArgumentsDoneEvent,
    ResponseInProgressEvent, ResponseOutputItemAddedEvent, ResponseOutputItemDoneEvent,
    ResponseStreamEvent, ResponseTextDeltaEvent, ResponseUsage, Status, SummaryPart,
    SummaryTextContent,
};
use axum::{
    Json, Router,
    extract::State,
    response::sse::{Event, Sse},
    routing::{get, post},
};
use futures::StreamExt as _;
use futures::stream::Stream;
use gemini_rust::tools::ToolConfig;
use gemini_rust::{
    Content, FunctionCall, FunctionDeclaration, FunctionResponse, Gemini, Model, Part, Role,
    ThinkingConfig, ThinkingLevel, Tool as GTool,
};
use serde::Deserialize;
use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;

/// Hand-typed codex `/v1/responses` request shape.
#[derive(Deserialize)]
struct CodexReq {
    /// Requested model id, defaulting when absent.
    #[serde(default)]
    model: Option<String>,
    /// System instructions for the turn.
    #[serde(default)]
    instructions: Option<String>,
    /// Stateless per-turn conversation input array.
    #[serde(default)]
    input: Vec<CodexInput>,
    /// Function tools declared for the turn.
    #[serde(default)]
    tools: Vec<CodexTool>,
    /// Reasoning-effort control.
    #[serde(default)]
    reasoning: Option<CodexReasoning>,
}
/// Tagged union of codex per-turn input items.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexInput {
    /// A user/assistant/system/developer message.
    Message {
        /// Author role of the message.
        role: CodexRole,
        /// Message content parts.
        #[serde(default)]
        content: Vec<CodexContent>,
    },
    /// A replayed reasoning item carrying the thought signature.
    Reasoning {
        /// Round-tripped thought signature.
        #[serde(default)]
        encrypted_content: Option<String>,
    },
    /// A prior function call the model made.
    FunctionCall {
        /// Correlation id pairing call with output.
        #[serde(default)]
        call_id: String,
        /// Function name invoked.
        #[serde(default)]
        name: String,
        /// JSON-encoded call arguments.
        #[serde(default)]
        arguments: String,
    },
    /// The result of a prior function call.
    FunctionCallOutput {
        /// Correlation id pairing output with call.
        #[serde(default)]
        call_id: String,
        /// Tool output payload.
        #[serde(default)]
        output: String,
    },
    /// Any other input item kind.
    #[serde(other)]
    Other,
}
/// Message author role.
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum CodexRole {
    /// End user.
    User,
    /// Model assistant.
    Assistant,
    /// System role.
    System,
    /// Developer role.
    Developer,
    /// Any other role.
    #[serde(other)]
    Other,
}
/// Reasoning-effort level codex requests.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CodexEffort {
    /// Minimal thinking.
    Minimal,
    /// Low thinking.
    Low,
    /// Medium thinking.
    Medium,
    /// High thinking.
    High,
    /// Extra-high thinking (clamped to high).
    Xhigh,
}
/// Tool kind codex declares.
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum CodexToolKind {
    /// A function tool.
    Function,
    /// Any other tool kind.
    #[serde(other)]
    Other,
}
/// One content part of a codex message.
#[derive(Deserialize)]
struct CodexContent {
    /// Text body of the part.
    #[serde(default)]
    text: Option<String>,
}
/// A function tool declaration.
#[derive(Deserialize)]
struct CodexTool {
    /// Declared tool kind.
    #[serde(rename = "type")]
    kind: CodexToolKind,
    /// Tool name.
    #[serde(default)]
    name: String,
    /// Tool description.
    #[serde(default)]
    description: String,
    /// JSON-schema parameters.
    #[serde(default)]
    parameters: Option<Value>,
}
/// Reasoning control block.
#[derive(Deserialize)]
struct CodexReasoning {
    /// Requested reasoning effort.
    #[serde(default)]
    effort: Option<CodexEffort>,
}

/// Shared handler state carrying the BYOK key.
#[derive(Clone)]
struct AppState {
    /// Gemini API key.
    api_key: String,
}

/// Recursively strip Gemini-unsupported JSON-schema keywords from tool parameters.
fn sanitize_schema(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("additionalProperties");
            map.remove("$schema");
            for child in map.values_mut() {
                sanitize_schema(child);
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                sanitize_schema(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Reconstruct the gemini contents vector from the codex per-turn input array.
fn build_contents(req: &CodexReq) -> Vec<Content> {
    let mut names: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for item in &req.input {
        if let CodexInput::FunctionCall { call_id, name, .. } = item {
            names.insert(call_id.clone(), name.clone());
        }
    }
    let mut contents: Vec<Content> = Vec::new();
    let mut pending_sig: Option<String> = None;
    for item in &req.input {
        match item {
            CodexInput::Message { role, content } => {
                let txt: String = content
                    .iter()
                    .filter_map(|part| part.text.clone())
                    .collect::<String>();
                if txt.is_empty() {
                    continue;
                }
                contents.push(
                    Content::text(txt).with_role(if *role == CodexRole::Assistant {
                        Role::Model
                    } else {
                        Role::User
                    }),
                );
            }
            CodexInput::Reasoning { encrypted_content } => {
                if let Some(enc) = encrypted_content {
                    if !enc.is_empty() {
                        pending_sig = Some(enc.clone());
                    }
                }
            }
            CodexInput::FunctionCall {
                name, arguments, ..
            } => {
                let args: Value = serde_json::from_str(arguments).unwrap_or_default();
                let sig = pending_sig
                    .take()
                    .unwrap_or_else(|| "skip_thought_signature_validator".into());
                contents.push(
                    Content::function_call_with_thought(FunctionCall::new(name, args), sig)
                        .with_role(Role::Model),
                );
            }
            CodexInput::FunctionCallOutput { call_id, output } => {
                let name = names
                    .get(call_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".into());
                contents.push(
                    Content::function_response(FunctionResponse::new(
                        name,
                        serde_json::json!({"output": output}),
                    ))
                    .with_role(Role::User),
                );
            }
            CodexInput::Other => {}
        }
    }
    contents
}

/// Entry point: bind the bridge and serve.
#[tokio::main]
async fn main() {
    let Ok(api_key) = std::env::var("GEMINI_API_KEY") else {
        let _ = writeln!(std::io::stderr(), "GEMINI_API_KEY env required (no fallback)");
        return;
    };
    let app = Router::new()
        .route("/v1/responses", post(responses))
        .route("/health/liveliness", get(|| async { "ok" }))
        .with_state(AppState { api_key });
    let Ok(port) = std::env::var("PORT") else {
        let _ = writeln!(std::io::stderr(), "PORT env required (no fallback)");
        return;
    };
    let Ok(listener) = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await else {
        let _ = writeln!(std::io::stderr(), "bind failed on :{port}");
        return;
    };
    let _ = writeln!(std::io::stderr(), "typed bridge on :{port}");
    if let Err(err) = axum::serve(listener, app).await {
        let _ = writeln!(std::io::stderr(), "serve failed: {err}");
    }
}

/// Build the gemini request builder from the codex request + reconstructed contents.
fn build_request(
    client: Gemini,
    req: &CodexReq,
    contents: Vec<Content>,
) -> gemini_rust::ContentBuilder {
    let mut builder = client.generate_content();
    builder.contents = contents;
    if let Some(instructions) = &req.instructions {
        builder = builder.with_system_prompt(instructions.clone());
    }
    for tool in &req.tools {
        if tool.kind != CodexToolKind::Function {
            continue;
        }
        let mut params = tool
            .parameters
            .clone()
            .unwrap_or_else(|| serde_json::json!({"type":"object","properties":{}}));
        sanitize_schema(&mut params);
        builder = builder.with_tool(GTool::new(
            FunctionDeclaration::new(&tool.name, &tool.description, None)
                .with_parameters_value(params),
        ));
    }
    builder = builder.with_tool(GTool::google_search());
    builder = builder.with_tool(GTool::url_context());
    builder = builder.with_tool_config(ToolConfig {
        include_server_side_tool_invocations: Some(true),
        ..Default::default()
    });
    let mut thinking = ThinkingConfig::new().with_thoughts_included(true);
    if let Some(reasoning) = &req.reasoning {
        if let Some(effort) = reasoning.effort {
            let level = match effort {
                CodexEffort::Minimal => ThinkingLevel::Minimal,
                CodexEffort::Low => ThinkingLevel::Low,
                CodexEffort::Medium => ThinkingLevel::Medium,
                CodexEffort::High | CodexEffort::Xhigh => ThinkingLevel::High,
            };
            thinking = thinking.with_thinking_level(level);
        }
    }
    builder = builder.with_thinking_config(thinking);
    builder
}

/// Serialize an event to SSE; on serialize failure emit empty data.
fn to_event(event: &ResponseStreamEvent) -> Result<Event, Infallible> {
    let data = serde_json::to_string(event).unwrap_or_default();
    Ok(Event::default().data(data))
}

/// Codex `/v1/responses` handler: translate to gemini, stream typed responses events.
async fn responses(
    State(state): State<AppState>,
    Json(req): Json<CodexReq>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| "gemini-3.5-flash".into());
    let contents = build_contents(&req);
    let api_model = if model.starts_with("models/") {
        model.clone()
    } else {
        format!("models/{model}")
    };
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(64);
    let response_id = format!("resp_{}", uuid::Uuid::new_v4().simple());

    let Ok(client) = Gemini::with_model(state.api_key, Model::Custom(api_model)) else {
        tokio::spawn(async move {
            let response = make_response(&response_id, &model, Status::Failed, vec![], None);
            let event = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
                sequence_number: 1,
                response,
            });
            let _ = sender.send(to_event(&event)).await;
        });
        return Sse::new(ReceiverStream::new(receiver));
    };
    let builder = build_request(client, &req, contents);

    tokio::spawn(async move {
        stream_responses(builder, sender, response_id, model).await;
    });
    Sse::new(ReceiverStream::new(receiver))
}

/// Build the responses `Response` envelope shared across stream events.
fn make_response(
    response_id: &str,
    model: &str,
    status: Status,
    output: Vec<OutputItem>,
    usage: Option<ResponseUsage>,
) -> Response {
    Response {
        background: None,
        billing: None,
        conversation: None,
        created_at: 0,
        completed_at: None,
        error: None,
        id: response_id.to_owned(),
        incomplete_details: None,
        instructions: None,
        max_output_tokens: None,
        metadata: None,
        model: model.to_owned(),
        object: "response".into(),
        output,
        parallel_tool_calls: None,
        previous_response_id: None,
        prompt: None,
        prompt_cache_key: None,
        prompt_cache_retention: None,
        reasoning: None,
        safety_identifier: None,
        service_tier: None,
        status,
        temperature: None,
        text: None,
        tool_choice: None,
        tools: None,
        top_logprobs: None,
        top_p: None,
        truncation: None,
        usage,
    }
}

/// Drive the gemini stream and emit the typed responses event sequence.
async fn stream_responses(
    builder: gemini_rust::ContentBuilder,
    sender: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    response_id: String,
    model: String,
) {
    let mut seq = 0_u64;
    macro_rules! send {
        ($event:expr) => {
            if sender.send($event).await.is_err() {
                return;
            }
        };
    }

    seq = seq.wrapping_add(1);
    send!(to_event(&ResponseStreamEvent::ResponseCreated(
        ResponseCreatedEvent {
            sequence_number: seq,
            response: make_response(
                &response_id,
                &model,
                Status::InProgress,
                vec![],
                None
            ),
        }
    )));
    seq = seq.wrapping_add(1);
    send!(to_event(&ResponseStreamEvent::ResponseInProgress(
        ResponseInProgressEvent {
            sequence_number: seq,
            response: make_response(
                &response_id,
                &model,
                Status::InProgress,
                vec![],
                None
            ),
        }
    )));

    let mut stream = if let Ok(stream) = builder.execute_stream().await {
        stream
    } else {
        seq = seq.wrapping_add(1);
        send!(to_event(&ResponseStreamEvent::ResponseFailed(
            ResponseFailedEvent {
                sequence_number: seq,
                response: make_response(&response_id, &model, Status::Failed, vec![], None),
            }
        )));
        return;
    };
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut rsn_sig = String::new();
    let mut out_items: Vec<OutputItem> = Vec::new();
    let mut fcs: Vec<(String, String)> = Vec::new();
    let mut usage: Option<ResponseUsage> = None;
    let mut got_finish = false;
    let mut finish: Option<gemini_rust::FinishReason> = None;
    let mut output_index = 0_u32;
    let mut rsn_emitted = false;
    let mut msg_open = false;
    let mut msg_id = String::new();
    let mut msg_oi = 0_u32;
    macro_rules! flush_reasoning {
        () => {
            if !rsn_emitted && (!reasoning.is_empty() || !rsn_sig.is_empty()) {
                rsn_emitted = true;
                let reasoning_item = ReasoningItem {
                    id: Some(format!("rs_{}", uuid::Uuid::new_v4().simple())),
                    summary: vec![SummaryPart::SummaryText(SummaryTextContent {
                        text: reasoning.clone(),
                    })],
                    content: None,
                    encrypted_content: Some(rsn_sig.clone()),
                    status: Some(OutputStatus::Completed),
                };
                seq = seq.wrapping_add(1);
                send!(to_event(&ResponseStreamEvent::ResponseOutputItemAdded(
                    ResponseOutputItemAddedEvent {
                        sequence_number: seq,
                        output_index,
                        item: OutputItem::Reasoning(reasoning_item.clone()),
                    }
                )));
                seq = seq.wrapping_add(1);
                send!(to_event(&ResponseStreamEvent::ResponseOutputItemDone(
                    ResponseOutputItemDoneEvent {
                        sequence_number: seq,
                        output_index,
                        item: OutputItem::Reasoning(reasoning_item.clone()),
                    }
                )));
                out_items.push(OutputItem::Reasoning(reasoning_item));
                output_index = output_index.wrapping_add(1);
            }
        };
    }
    while let Some(item) = stream.next().await {
        let chunk = match item {
            Ok(chunk) => chunk,
            Err(_) => break,
        };
        if let Some(candidate) = chunk.candidates.into_iter().next() {
            if let Some(parts) = candidate.content.parts {
                for part in parts {
                    match part {
                        Part::Text {
                            text: part_text,
                            thought: Some(true),
                            thought_signature,
                        } => {
                            reasoning.push_str(&part_text);
                            if let Some(signature) = thought_signature {
                                rsn_sig = signature;
                            }
                        }
                        Part::Text {
                            text: part_text, ..
                        } if !part_text.is_empty() => {
                            flush_reasoning!();
                            if !msg_open {
                                msg_open = true;
                                msg_oi = output_index;
                                output_index = output_index.wrapping_add(1);
                                msg_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
                                let message = OutputMessage {
                                    content: vec![],
                                    id: msg_id.clone(),
                                    role: AssistantRole::Assistant,
                                    phase: None,
                                    status: OutputStatus::InProgress,
                                };
                                seq = seq.wrapping_add(1);
                                send!(to_event(
                                    &ResponseStreamEvent::ResponseOutputItemAdded(
                                        ResponseOutputItemAddedEvent {
                                            sequence_number: seq,
                                            output_index: msg_oi,
                                            item: OutputItem::Message(message),
                                        }
                                    )
                                ));
                            }
                            text.push_str(&part_text);
                            seq = seq.wrapping_add(1);
                            send!(to_event(&ResponseStreamEvent::ResponseOutputTextDelta(
                                ResponseTextDeltaEvent {
                                    sequence_number: seq,
                                    item_id: msg_id.clone(),
                                    output_index: msg_oi,
                                    content_index: 0,
                                    delta: part_text,
                                    logprobs: None,
                                }
                            )));
                        }
                        Part::FunctionCall {
                            function_call,
                            thought_signature,
                        } => {
                            if let Some(signature) = thought_signature {
                                rsn_sig = signature;
                            }
                            flush_reasoning!();
                            fcs.push((
                                function_call.name,
                                serde_json::to_string(&function_call.args)
                                    .unwrap_or_else(|_| "{}".into()),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            if let Some(finish_reason) = candidate.finish_reason {
                got_finish = true;
                finish = Some(finish_reason);
            }
        }
        if let Some(meta) = chunk.usage_metadata {
            usage = Some(ResponseUsage {
                input_tokens: u32::try_from(meta.prompt_token_count.unwrap_or(0)).unwrap_or(0),
                input_tokens_details: InputTokenDetails {
                    cached_tokens: u32::try_from(meta.cached_content_token_count.unwrap_or(0))
                        .unwrap_or(0),
                },
                output_tokens: u32::try_from(meta.candidates_token_count.unwrap_or(0))
                    .unwrap_or(0),
                output_tokens_details: OutputTokenDetails {
                    reasoning_tokens: u32::try_from(meta.thoughts_token_count.unwrap_or(0))
                        .unwrap_or(0),
                },
                total_tokens: u32::try_from(meta.total_token_count.unwrap_or(0)).unwrap_or(0),
            });
        }
    }
    flush_reasoning!();
    if msg_open {
        let message = OutputMessage {
            content: vec![OutputMessageContent::OutputText(OutputTextContent {
                text: text.clone(),
                annotations: vec![],
                logprobs: None,
            })],
            id: msg_id.clone(),
            role: AssistantRole::Assistant,
            phase: None,
            status: OutputStatus::Completed,
        };
        seq = seq.wrapping_add(1);
        send!(to_event(&ResponseStreamEvent::ResponseOutputItemDone(
            ResponseOutputItemDoneEvent {
                sequence_number: seq,
                output_index: msg_oi,
                item: OutputItem::Message(message.clone()),
            }
        )));
        let insert_at = if rsn_emitted {
            1.min(out_items.len())
        } else {
            0
        };
        out_items.insert(insert_at, OutputItem::Message(message));
    }
    for (name, args) in fcs {
        let fc_id = format!("fc_{}", uuid::Uuid::new_v4().simple());
        let function_call = FunctionToolCall {
            arguments: args.clone(),
            call_id: format!("call_{}", uuid::Uuid::new_v4().simple()),
            namespace: None,
            name,
            id: Some(fc_id.clone()),
            status: Some(OutputStatus::Completed),
        };
        seq = seq.wrapping_add(1);
        send!(to_event(&ResponseStreamEvent::ResponseOutputItemAdded(
            ResponseOutputItemAddedEvent {
                sequence_number: seq,
                output_index,
                item: OutputItem::FunctionCall(function_call.clone()),
            }
        )));
        seq = seq.wrapping_add(1);
        send!(to_event(
            &ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
                ResponseFunctionCallArgumentsDeltaEvent {
                    sequence_number: seq,
                    item_id: fc_id.clone(),
                    output_index,
                    delta: args.clone(),
                }
            )
        ));
        seq = seq.wrapping_add(1);
        send!(to_event(
            &ResponseStreamEvent::ResponseFunctionCallArgumentsDone(
                ResponseFunctionCallArgumentsDoneEvent {
                    name: None,
                    sequence_number: seq,
                    item_id: fc_id,
                    output_index,
                    arguments: args,
                }
            )
        ));
        seq = seq.wrapping_add(1);
        send!(to_event(&ResponseStreamEvent::ResponseOutputItemDone(
            ResponseOutputItemDoneEvent {
                sequence_number: seq,
                output_index,
                item: OutputItem::FunctionCall(function_call.clone()),
            }
        )));
        out_items.push(OutputItem::FunctionCall(function_call));
        output_index = output_index.wrapping_add(1);
    }
    use gemini_rust::FinishReason as FR;
    let incomplete = match finish {
        Some(FR::MaxTokens) => Some("max_output_tokens"),
        Some(FR::Safety | FR::Recitation | FR::ImageSafety) => Some("content_filter"),
        _ => None,
    };
    if got_finish {
        if let Some(reason) = incomplete {
            let mut resp = make_response(
                &response_id,
                &model,
                Status::Incomplete,
                out_items,
                usage,
            );
            resp.incomplete_details =
                Some(async_openai::types::responses::IncompleteDetails { reason: reason.into() });
            seq = seq.wrapping_add(1);
            send!(to_event(&ResponseStreamEvent::ResponseIncomplete(
                async_openai::types::responses::ResponseIncompleteEvent {
                    sequence_number: seq,
                    response: resp,
                }
            )));
        } else {
            seq = seq.wrapping_add(1);
            send!(to_event(&ResponseStreamEvent::ResponseCompleted(
                ResponseCompletedEvent {
                    sequence_number: seq,
                    response: make_response(
                        &response_id,
                        &model,
                        Status::Completed,
                        out_items,
                        usage
                    ),
                }
            )));
        }
    } else {
        seq = seq.wrapping_add(1);
        send!(to_event(&ResponseStreamEvent::ResponseFailed(
            ResponseFailedEvent {
                sequence_number: seq,
                response: make_response(
                    &response_id,
                    &model,
                    Status::Failed,
                    out_items,
                    usage
                ),
            }
        )));
    }
}
