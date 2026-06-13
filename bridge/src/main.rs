//! MAX-TYPESAFE bridge: async-openai `CreateResponse` (typed request) -> gemini-rust (typed gemini) ->
//! async-openai `ResponseStreamEvent` (typed emit). No raw `serde_json::Value` for request/response shapes.
use core::convert::Infallible;
use core::mem::take;
use std::collections::HashMap;
use std::env::var;
use std::io::{Write as _, stderr};

use async_openai::types::responses::{
    AssistantRole, FunctionToolCall, IncompleteDetails, InputTokenDetails, OutputItem,
    OutputMessage, OutputMessageContent, OutputStatus, OutputTextContent, OutputTokenDetails,
    ReasoningItem, Response, ResponseCompletedEvent, ResponseCreatedEvent, ResponseFailedEvent,
    ResponseFunctionCallArgumentsDeltaEvent, ResponseFunctionCallArgumentsDoneEvent,
    ResponseInProgressEvent, ResponseIncompleteEvent, ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent, ResponseStreamEvent, ResponseTextDeltaEvent, ResponseUsage,
    Status, SummaryPart, SummaryTextContent,
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
    Content, ContentBuilder, FinishReason, FunctionCall, FunctionDeclaration, FunctionResponse,
    Gemini, GenerationStream, Model, Part, Role, ThinkingConfig, ThinkingLevel, Tool as GTool,
    UsageMetadata,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Sender, channel};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Reasoning-effort level codex requests.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CodexEffort {
    /// High thinking.
    High,
    /// Low thinking.
    Low,
    /// Medium thinking.
    Medium,
    /// Minimal thinking.
    Minimal,
    /// Extra-high thinking (clamped to high).
    Xhigh,
}
/// Tagged union of codex per-turn input items.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexInput {
    /// A prior function call the model made.
    FunctionCall {
        /// JSON-encoded call arguments.
        #[serde(default)]
        arguments: String,
        /// Correlation id pairing call with output.
        #[serde(default)]
        call_id: String,
        /// Function name invoked.
        #[serde(default)]
        name: String,
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
    /// A user/assistant/system/developer message.
    Message {
        /// Message content parts.
        #[serde(default)]
        content: Vec<CodexContent>,
        /// Author role of the message.
        role: CodexRole,
    },
    /// A replayed reasoning item carrying the thought signature.
    Reasoning {
        /// Round-tripped thought signature.
        #[serde(default)]
        encrypted_content: Option<String>,
    },
    /// Any other input item kind.
    #[serde(other)]
    Other,
}
/// Message author role.
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum CodexRole {
    /// Model assistant.
    Assistant,
    /// Developer role.
    Developer,
    /// System role.
    System,
    /// End user.
    User,
    /// Any other role.
    #[serde(other)]
    Other,
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

/// Shared handler state carrying the BYOK key.
#[derive(Clone)]
struct AppState {
    /// Gemini API key.
    api_key: String,
}
/// One content part of a codex message.
#[derive(Deserialize)]
struct CodexContent {
    /// Text body of the part.
    #[serde(default)]
    text: Option<String>,
}
/// Reasoning control block.
#[derive(Deserialize)]
struct CodexReasoning {
    /// Requested reasoning effort.
    #[serde(default)]
    effort: Option<CodexEffort>,
}
/// Hand-typed codex `/v1/responses` request shape.
#[derive(Deserialize)]
struct CodexReq {
    /// Stateless per-turn conversation input array.
    #[serde(default)]
    input: Vec<CodexInput>,
    /// System instructions for the turn.
    #[serde(default)]
    instructions: Option<String>,
    /// Requested model id, defaulting when absent.
    #[serde(default)]
    model: Option<String>,
    /// Reasoning-effort control.
    #[serde(default)]
    reasoning: Option<CodexReasoning>,
    /// Function tools declared for the turn.
    #[serde(default)]
    tools: Vec<CodexTool>,
}
/// A function tool declaration.
#[derive(Deserialize)]
struct CodexTool {
    /// Tool description.
    #[serde(default)]
    description: String,
    /// Declared tool kind.
    #[serde(rename = "type")]
    kind: CodexToolKind,
    /// Tool name.
    #[serde(default)]
    name: String,
    /// JSON-schema parameters.
    #[serde(default)]
    parameters: Option<Value>,
}
/// Mutable accumulator threaded through the gemini stream loop.
struct StreamState {
    /// Pending function calls (name, arguments).
    fcs: Vec<(String, String)>,
    /// The observed finish reason.
    finish: Option<FinishReason>,
    /// Whether a finish reason was observed.
    got_finish: bool,
    /// The open message item id.
    msg_id: String,
    /// The open message output index.
    msg_oi: u32,
    /// Whether the assistant message item is open.
    msg_open: bool,
    /// Completed output items in emit order.
    out_items: Vec<OutputItem>,
    /// Next output index to assign.
    output_index: u32,
    /// Accumulated reasoning text.
    reasoning: String,
    /// Whether the reasoning item was emitted.
    rsn_emitted: bool,
    /// Latest captured thought signature.
    rsn_sig: String,
    /// Monotonic SSE sequence number.
    seq: u64,
    /// Accumulated visible assistant text.
    text: String,
    /// Token usage once observed.
    usage: Option<ResponseUsage>,
}

/// Discard a value to satisfy must-use / non-binding-let lints without altering behavior.
fn discard<T>(_value: T) {}

/// Build the responses `Response` envelope shared across stream events.
fn make_response(
    response_id: &str,
    model: &str,
    status: Status,
    output: Vec<OutputItem>,
    usage: Option<ResponseUsage>,
) -> Response {
    return Response {
        background: None,
        billing: None,
        conversation: None,
        created_at: 0_u64,
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
    };
}

/// Serialize an event to SSE; on serialize failure emit empty data.
fn to_event(event: &ResponseStreamEvent) -> Event {
    let data = serde_json::to_string(event).unwrap_or_default();
    return Event::default().data(data);
}

/// Recursively strip Gemini-unsupported JSON-schema keywords from tool parameters.
fn sanitize_schema(value: &mut Value) {
    match *value {
        Value::Object(ref mut map) => {
            discard(map.remove("additionalProperties"));
            discard(map.remove("$schema"));
            for child in map.values_mut() {
                sanitize_schema(child);
            }
        }
        Value::Array(ref mut arr) => {
            for child in arr.iter_mut() {
                sanitize_schema(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Build the gemini contents one message item contributes.
fn push_message(contents: &mut Vec<Content>, role: &CodexRole, content: &[CodexContent]) {
    let txt: String = content
        .iter()
        .filter_map(|part| return part.text.clone())
        .collect::<String>();
    if txt.is_empty() {
        return;
    }
    let mapped_role = if *role == CodexRole::Assistant {
        Role::Model
    } else {
        Role::User
    };
    contents.push(Content::text(txt).with_role(mapped_role));
}

/// Reconstruct the gemini contents vector from the codex per-turn input array.
fn build_contents(req: &CodexReq) -> Vec<Content> {
    let mut names: HashMap<String, String> = HashMap::new();
    for item in &req.input {
        if let CodexInput::FunctionCall { call_id, name, .. } = item {
            discard(names.insert(call_id.clone(), name.clone()));
        }
    }
    let mut contents: Vec<Content> = Vec::new();
    let mut pending_sig: Option<String> = None;
    for item in &req.input {
        match item {
            CodexInput::Message { role, content } => {
                push_message(&mut contents, role, content);
            }
            CodexInput::Reasoning { encrypted_content } => {
                if let Some(enc) = encrypted_content
                    && !enc.is_empty()
                {
                    pending_sig = Some(enc.clone());
                }
            }
            CodexInput::FunctionCall {
                name, arguments, ..
            } => {
                let args: Value = serde_json::from_str(arguments).unwrap_or_default();
                let sig = pending_sig
                    .take()
                    .unwrap_or_else(|| return "skip_thought_signature_validator".into());
                contents.push(
                    Content::function_call_with_thought(FunctionCall::new(name, args), sig)
                        .with_role(Role::Model),
                );
            }
            CodexInput::FunctionCallOutput { call_id, output } => {
                let name = names
                    .get(call_id)
                    .cloned()
                    .unwrap_or_else(|| return "unknown".into());
                contents.push(
                    Content::function_response(FunctionResponse::new(
                        name,
                        serde_json::json!({ "output": output }),
                    ))
                    .with_role(Role::User),
                );
            }
            CodexInput::Other => {}
        }
    }
    return contents;
}

/// Map reasoning effort onto the gemini thinking level.
const fn effort_level(effort: CodexEffort) -> ThinkingLevel {
    return match effort {
        CodexEffort::Minimal => ThinkingLevel::Minimal,
        CodexEffort::Low => ThinkingLevel::Low,
        CodexEffort::Medium => ThinkingLevel::Medium,
        CodexEffort::High | CodexEffort::Xhigh => ThinkingLevel::High,
    };
}

/// Build the gemini request builder from the codex request + reconstructed contents.
fn build_request(client: &Gemini, req: &CodexReq, contents: Vec<Content>) -> ContentBuilder {
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
            .unwrap_or_else(|| return serde_json::json!({ "type": "object", "properties": {} }));
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
        ..ToolConfig::default()
    });
    let mut thinking = ThinkingConfig::new().with_thoughts_included(true);
    if let Some(reasoning) = &req.reasoning
        && let Some(effort) = reasoning.effort
    {
        thinking = thinking.with_thinking_level(effort_level(effort));
    }
    builder = builder.with_thinking_config(thinking);
    return builder;
}

/// Emit the two opening lifecycle events; returns false if the receiver closed.
async fn emit_open(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    response_id: &str,
    model: &str,
) -> bool {
    state.seq = state.seq.wrapping_add(1);
    let created = ResponseStreamEvent::ResponseCreated(ResponseCreatedEvent {
        sequence_number: state.seq,
        response: make_response(response_id, model, Status::InProgress, vec![], None),
    });
    if sender.send(Ok(to_event(&created))).await.is_err() {
        return false;
    }
    state.seq = state.seq.wrapping_add(1);
    let in_progress = ResponseStreamEvent::ResponseInProgress(ResponseInProgressEvent {
        sequence_number: state.seq,
        response: make_response(response_id, model, Status::InProgress, vec![], None),
    });
    return sender.send(Ok(to_event(&in_progress))).await.is_ok();
}

/// Emit the reasoning item if pending; returns false if the receiver closed.
async fn flush_reasoning(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
) -> bool {
    if state.rsn_emitted || (state.reasoning.is_empty() && state.rsn_sig.is_empty()) {
        return true;
    }
    state.rsn_emitted = true;
    let reasoning_item = ReasoningItem {
        id: Some(format!("rs_{}", Uuid::new_v4().simple())),
        summary: vec![SummaryPart::SummaryText(SummaryTextContent {
            text: state.reasoning.clone(),
        })],
        content: None,
        encrypted_content: Some(state.rsn_sig.clone()),
        status: Some(OutputStatus::Completed),
    };
    state.seq = state.seq.wrapping_add(1);
    let added = ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent {
        sequence_number: state.seq,
        output_index: state.output_index,
        item: OutputItem::Reasoning(reasoning_item.clone()),
    });
    if sender.send(Ok(to_event(&added))).await.is_err() {
        return false;
    }
    state.seq = state.seq.wrapping_add(1);
    let done = ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
        sequence_number: state.seq,
        output_index: state.output_index,
        item: OutputItem::Reasoning(reasoning_item.clone()),
    });
    if sender.send(Ok(to_event(&done))).await.is_err() {
        return false;
    }
    state.out_items.push(OutputItem::Reasoning(reasoning_item));
    state.output_index = state.output_index.wrapping_add(1);
    return true;
}

/// Handle a visible (non-thought) text part; returns false if the receiver closed.
async fn emit_text_part(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    part_text: String,
) -> bool {
    if !flush_reasoning(sender, state).await {
        return false;
    }
    if !state.msg_open {
        state.msg_open = true;
        state.msg_oi = state.output_index;
        state.output_index = state.output_index.wrapping_add(1);
        state.msg_id = format!("msg_{}", Uuid::new_v4().simple());
        let message = OutputMessage {
            content: vec![],
            id: state.msg_id.clone(),
            role: AssistantRole::Assistant,
            phase: None,
            status: OutputStatus::InProgress,
        };
        state.seq = state.seq.wrapping_add(1);
        let added = ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent {
            sequence_number: state.seq,
            output_index: state.msg_oi,
            item: OutputItem::Message(message),
        });
        if sender.send(Ok(to_event(&added))).await.is_err() {
            return false;
        }
    }
    state.text.push_str(&part_text);
    state.seq = state.seq.wrapping_add(1);
    let delta = ResponseStreamEvent::ResponseOutputTextDelta(ResponseTextDeltaEvent {
        sequence_number: state.seq,
        item_id: state.msg_id.clone(),
        output_index: state.msg_oi,
        content_index: 0_u32,
        delta: part_text,
        logprobs: None,
    });
    return sender.send(Ok(to_event(&delta))).await.is_ok();
}

/// Process one gemini part; returns false if the receiver closed.
async fn handle_part(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    part: Part,
) -> bool {
    match part {
        Part::Text {
            text: part_text,
            thought: Some(true),
            thought_signature,
        } => {
            state.reasoning.push_str(&part_text);
            if let Some(signature) = thought_signature {
                state.rsn_sig = signature;
            }
            return true;
        }
        Part::Text {
            text: part_text, ..
        } if !part_text.is_empty() => {
            return emit_text_part(sender, state, part_text).await;
        }
        Part::FunctionCall {
            function_call,
            thought_signature,
        } => {
            if let Some(signature) = thought_signature {
                state.rsn_sig = signature;
            }
            if !flush_reasoning(sender, state).await {
                return false;
            }
            state.fcs.push((
                function_call.name,
                serde_json::to_string(&function_call.args).unwrap_or_else(|_| return "{}".into()),
            ));
            return true;
        }
        Part::Text { .. }
        | Part::InlineData { .. }
        | Part::FunctionResponse { .. }
        | Part::ToolCall { .. }
        | Part::ToolResponse { .. }
        | Part::FileData { .. }
        | Part::ExecutableCode { .. }
        | Part::CodeExecutionResult { .. } => return true,
    }
}

/// Map gemini usage metadata into the responses usage shape.
fn map_usage(meta: &UsageMetadata) -> ResponseUsage {
    return ResponseUsage {
        input_tokens: u32::try_from(meta.prompt_token_count.unwrap_or(0)).unwrap_or(0),
        input_tokens_details: InputTokenDetails {
            cached_tokens: u32::try_from(meta.cached_content_token_count.unwrap_or(0)).unwrap_or(0),
        },
        output_tokens: u32::try_from(meta.candidates_token_count.unwrap_or(0)).unwrap_or(0),
        output_tokens_details: OutputTokenDetails {
            reasoning_tokens: u32::try_from(meta.thoughts_token_count.unwrap_or(0)).unwrap_or(0),
        },
        total_tokens: u32::try_from(meta.total_token_count.unwrap_or(0)).unwrap_or(0),
    };
}

/// Consume the gemini stream into the accumulator; returns false if the receiver closed.
async fn consume_stream(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    mut stream: GenerationStream,
) -> bool {
    while let Some(item) = stream.next().await {
        let Ok(chunk) = item else { break };
        if let Some(candidate) = chunk.candidates.into_iter().next() {
            if let Some(parts) = candidate.content.parts {
                for part in parts {
                    if !handle_part(sender, state, part).await {
                        return false;
                    }
                }
            }
            if let Some(finish_reason) = candidate.finish_reason {
                state.got_finish = true;
                state.finish = Some(finish_reason);
            }
        }
        if let Some(meta) = &chunk.usage_metadata {
            state.usage = Some(map_usage(meta));
        }
    }
    return true;
}

/// Close the open assistant message; returns false if the receiver closed.
async fn close_message(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
) -> bool {
    if !state.msg_open {
        return true;
    }
    let message = OutputMessage {
        content: vec![OutputMessageContent::OutputText(OutputTextContent {
            text: state.text.clone(),
            annotations: vec![],
            logprobs: None,
        })],
        id: state.msg_id.clone(),
        role: AssistantRole::Assistant,
        phase: None,
        status: OutputStatus::Completed,
    };
    state.seq = state.seq.wrapping_add(1);
    let done = ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
        sequence_number: state.seq,
        output_index: state.msg_oi,
        item: OutputItem::Message(message.clone()),
    });
    if sender.send(Ok(to_event(&done))).await.is_err() {
        return false;
    }
    let insert_at = if state.rsn_emitted {
        1_usize.min(state.out_items.len())
    } else {
        0_usize
    };
    state
        .out_items
        .insert(insert_at, OutputItem::Message(message));
    return true;
}

/// Emit the four events for one pending function call; returns false if the receiver closed.
async fn emit_function_call(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    name: String,
    args: String,
) -> bool {
    let fc_id = format!("fc_{}", Uuid::new_v4().simple());
    let function_call = FunctionToolCall {
        arguments: args.clone(),
        call_id: format!("call_{}", Uuid::new_v4().simple()),
        namespace: None,
        name,
        id: Some(fc_id.clone()),
        status: Some(OutputStatus::Completed),
    };
    state.seq = state.seq.wrapping_add(1);
    let added = ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent {
        sequence_number: state.seq,
        output_index: state.output_index,
        item: OutputItem::FunctionCall(function_call.clone()),
    });
    if sender.send(Ok(to_event(&added))).await.is_err() {
        return false;
    }
    state.seq = state.seq.wrapping_add(1);
    let delta = ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
        ResponseFunctionCallArgumentsDeltaEvent {
            sequence_number: state.seq,
            item_id: fc_id.clone(),
            output_index: state.output_index,
            delta: args.clone(),
        },
    );
    if sender.send(Ok(to_event(&delta))).await.is_err() {
        return false;
    }
    state.seq = state.seq.wrapping_add(1);
    let args_done = ResponseStreamEvent::ResponseFunctionCallArgumentsDone(
        ResponseFunctionCallArgumentsDoneEvent {
            name: None,
            sequence_number: state.seq,
            item_id: fc_id,
            output_index: state.output_index,
            arguments: args,
        },
    );
    if sender.send(Ok(to_event(&args_done))).await.is_err() {
        return false;
    }
    state.seq = state.seq.wrapping_add(1);
    let item_done = ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
        sequence_number: state.seq,
        output_index: state.output_index,
        item: OutputItem::FunctionCall(function_call.clone()),
    });
    if sender.send(Ok(to_event(&item_done))).await.is_err() {
        return false;
    }
    state
        .out_items
        .push(OutputItem::FunctionCall(function_call));
    state.output_index = state.output_index.wrapping_add(1);
    return true;
}

/// Map the gemini finish reason onto the responses incomplete reason, if any.
const fn incomplete_reason(finish: Option<&FinishReason>) -> Option<&'static str> {
    return match finish {
        Some(&FinishReason::MaxTokens) => Some("max_output_tokens"),
        Some(&(FinishReason::Safety | FinishReason::Recitation | FinishReason::ImageSafety)) => {
            Some("content_filter")
        }
        Some(
            &(FinishReason::FinishReasonUnspecified
            | FinishReason::Stop
            | FinishReason::Language
            | FinishReason::Other
            | FinishReason::Blocklist
            | FinishReason::ProhibitedContent
            | FinishReason::Spii
            | FinishReason::MalformedFunctionCall
            | FinishReason::UnexpectedToolCall
            | FinishReason::TooManyToolCalls),
        )
        | None => None,
    };
}

/// Emit the terminal lifecycle event based on finish state.
async fn emit_terminal(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    response_id: &str,
    model: &str,
) {
    let out_items = take(&mut state.out_items);
    let usage = state.usage.take();
    if !state.got_finish {
        state.seq = state.seq.wrapping_add(1);
        let failed = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
            sequence_number: state.seq,
            response: make_response(response_id, model, Status::Failed, out_items, usage),
        });
        discard(sender.send(Ok(to_event(&failed))).await);
        return;
    }
    if let Some(reason) = incomplete_reason(state.finish.as_ref()) {
        let mut resp = make_response(response_id, model, Status::Incomplete, out_items, usage);
        resp.incomplete_details = Some(IncompleteDetails {
            reason: reason.into(),
        });
        state.seq = state.seq.wrapping_add(1);
        let incomplete = ResponseStreamEvent::ResponseIncomplete(ResponseIncompleteEvent {
            sequence_number: state.seq,
            response: resp,
        });
        discard(sender.send(Ok(to_event(&incomplete))).await);
        return;
    }
    state.seq = state.seq.wrapping_add(1);
    let completed = ResponseStreamEvent::ResponseCompleted(ResponseCompletedEvent {
        sequence_number: state.seq,
        response: make_response(response_id, model, Status::Completed, out_items, usage),
    });
    discard(sender.send(Ok(to_event(&completed))).await);
}

/// Drive the gemini stream and emit the typed responses event sequence.
async fn stream_responses(
    builder: ContentBuilder,
    sender: Sender<Result<Event, Infallible>>,
    response_id: &str,
    model: &str,
) {
    let mut state = StreamState {
        seq: 0_u64,
        text: String::new(),
        reasoning: String::new(),
        rsn_sig: String::new(),
        out_items: Vec::new(),
        fcs: Vec::new(),
        usage: None,
        got_finish: false,
        finish: None,
        output_index: 0_u32,
        rsn_emitted: false,
        msg_open: false,
        msg_id: String::new(),
        msg_oi: 0_u32,
    };

    if !emit_open(&sender, &mut state, response_id, model).await {
        return;
    }

    let Ok(stream) = builder.execute_stream().await else {
        state.seq = state.seq.wrapping_add(1);
        let failed = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
            sequence_number: state.seq,
            response: make_response(response_id, model, Status::Failed, vec![], None),
        });
        discard(sender.send(Ok(to_event(&failed))).await);
        return;
    };

    if !consume_stream(&sender, &mut state, stream).await {
        return;
    }
    if !flush_reasoning(&sender, &mut state).await {
        return;
    }
    if !close_message(&sender, &mut state).await {
        return;
    }
    let fcs = take(&mut state.fcs);
    for (name, args) in fcs {
        if !emit_function_call(&sender, &mut state, name, args).await {
            return;
        }
    }
    emit_terminal(&sender, &mut state, response_id, model).await;
}

/// Codex `/v1/responses` handler: translate to gemini, stream typed responses events.
async fn responses(
    State(state): State<AppState>,
    Json(req): Json<CodexReq>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| return "gemini-3.5-flash".into());
    let contents = build_contents(&req);
    let api_model = if model.starts_with("models/") {
        model.clone()
    } else {
        format!("models/{model}")
    };
    let (sender, receiver) = channel::<Result<Event, Infallible>>(64);
    let response_id = format!("resp_{}", Uuid::new_v4().simple());

    let Ok(client) = Gemini::with_model(state.api_key, Model::Custom(api_model)) else {
        discard(tokio::spawn(async move {
            let response = make_response(&response_id, &model, Status::Failed, vec![], None);
            let event = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
                sequence_number: 1_u64,
                response,
            });
            discard(sender.send(Ok(to_event(&event))).await);
        }));
        return Sse::new(ReceiverStream::new(receiver));
    };
    let builder = build_request(&client, &req, contents);

    discard(tokio::spawn(async move {
        stream_responses(builder, sender, &response_id, &model).await;
    }));
    return Sse::new(ReceiverStream::new(receiver));
}

/// Entry point: bind the bridge and serve.
#[tokio::main]
async fn main() {
    let Ok(api_key) = var("GEMINI_API_KEY") else {
        discard(writeln!(
            stderr(),
            "GEMINI_API_KEY env required (no fallback)"
        ));
        return;
    };
    let app = Router::new()
        .route("/v1/responses", post(responses))
        .route("/health/liveliness", get(async || return "ok"))
        .with_state(AppState { api_key });
    let Ok(port) = var("PORT") else {
        discard(writeln!(stderr(), "PORT env required (no fallback)"));
        return;
    };
    let Ok(listener) = TcpListener::bind(format!("0.0.0.0:{port}")).await else {
        discard(writeln!(stderr(), "bind failed on :{port}"));
        return;
    };
    discard(writeln!(stderr(), "typed bridge on :{port}"));
    if let Err(err) = axum::serve(listener, app).await {
        discard(writeln!(stderr(), "serve failed: {err}"));
    }
}
