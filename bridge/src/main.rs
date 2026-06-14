//! MAX-TYPESAFE bridge: async-openai `CreateResponse` (typed request) -> gemini-rust (typed gemini)
//! -> async-openai `ResponseStreamEvent` (typed emit). No raw `serde_json::Value` for
//! request/response shapes.
use core::{convert::Infallible, mem::take};
use std::{
    collections::HashMap,
    env::var,
    io::{Write as _, stderr},
};

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
use futures::{StreamExt as _, stream::Stream};
use gemini_rust::{
    Candidate, Content, ContentBuilder, FinishReason, FunctionCall, FunctionDeclaration,
    FunctionResponse, Gemini, GenerationStream, Model, Part, Role, ThinkingConfig, ThinkingLevel,
    Tool as GTool, UsageMetadata, tools::ToolConfig,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    net::TcpListener,
    sync::mpsc::{Sender, channel},
};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Reasoning-effort level codex requests.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CodexEffort {
    /// Maps to gemini `thinkingLevel` High.
    High,
    /// Maps to gemini `thinkingLevel` Low.
    Low,
    /// Maps to gemini `thinkingLevel` Medium.
    Medium,
    /// Maps to gemini `thinkingLevel` Minimal.
    Minimal,
    /// Codex's xhigh; gemini rejects it, so it clamps to `thinkingLevel` High.
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
    /// Any unrecognized input item kind.
    #[serde(other)]
    Unknown,
}
/// Message author role.
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum CodexRole {
    /// Model assistant; maps to the gemini Model role.
    Assistant,
    /// Every non-assistant role (user, system, developer, unknown); maps to the gemini User role.
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
/// Whether a finish reason has been observed on the stream.
#[derive(PartialEq, Eq, Clone, Copy)]
enum FinishObserved {
    /// No finish reason seen yet.
    No,
    /// A finish reason was observed.
    Yes,
}
/// Whether the assistant message output item is open.
#[derive(PartialEq, Eq, Clone, Copy)]
enum MsgOpen {
    /// No message item open.
    Closed,
    /// A message item is open.
    Open,
}
/// Whether the reasoning output item has been emitted.
#[derive(PartialEq, Eq, Clone, Copy)]
enum RsnEmitted {
    /// Reasoning item not yet emitted.
    No,
    /// Reasoning item already emitted.
    Yes,
}
/// Mutable accumulator threaded through the gemini stream loop.
struct StreamState {
    /// Pending function calls (name, arguments).
    fcs: Vec<(String, String)>,
    /// The observed finish reason.
    finish: Option<FinishReason>,
    /// Whether a finish reason was observed.
    got_finish: FinishObserved,
    /// The open message item id.
    msg_id: String,
    /// The open message output index.
    msg_oi: u32,
    /// Whether the assistant message item is open.
    msg_open: MsgOpen,
    /// Completed output items in emit order.
    out_items: Vec<OutputItem>,
    /// Next output index to assign.
    output_index: u32,
    /// Accumulated reasoning text.
    reasoning: String,
    /// Whether the reasoning item was emitted.
    rsn_emitted: RsnEmitted,
    /// Latest captured thought signature.
    rsn_sig: String,
    /// Monotonic SSE sequence number.
    seq: u64,
    /// Accumulated visible assistant text.
    text: String,
    /// Token usage once observed.
    usage: Option<ResponseUsage>,
}

impl StreamState {
    /// Construct a zeroed accumulator at the start of a stream.
    const fn new() -> Self {
        return Self {
            seq: 0_u64,
            text: String::new(),
            reasoning: String::new(),
            rsn_sig: String::new(),
            out_items: Vec::new(),
            fcs: Vec::new(),
            usage: None,
            got_finish: FinishObserved::No,
            finish: None,
            output_index: 0_u32,
            rsn_emitted: RsnEmitted::No,
            msg_open: MsgOpen::Closed,
            msg_id: String::new(),
            msg_oi: 0_u32,
        };
    }
}

/// Borrowed response identity (id + model) shared across every emitted event.
#[derive(Clone, Copy)]
struct RespMeta<'meta> {
    /// The model id stamped on every event.
    model: &'meta str,
    /// The response id stamped on every event.
    response_id: &'meta str,
}

/// Discard a value to satisfy must-use / non-binding-let lints without altering behavior.
fn discard<T>(_value: T) {}

/// Build the responses `Response` envelope shared across stream events.
fn make_response(
    meta: RespMeta<'_>,
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
        id: meta.response_id.to_owned(),
        incomplete_details: None,
        instructions: None,
        max_output_tokens: None,
        metadata: None,
        model: meta.model.to_owned(),
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
        },
        Value::Array(ref mut arr) => {
            for child in arr.iter_mut() {
                sanitize_schema(child);
            }
        },
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {},
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

/// Push a prior function call (with its replayed thought signature) onto the contents.
fn push_function_call(
    contents: &mut Vec<Content>,
    pending_sig: &mut Option<String>,
    name: &str,
    arguments: &str,
) {
    let args: Value = serde_json::from_str(arguments).unwrap_or_default();
    let sig = pending_sig
        .take()
        .unwrap_or_else(|| return "skip_thought_signature_validator".into());
    contents.push(
        Content::function_call_with_thought(FunctionCall::new(name, args), sig)
            .with_role(Role::Model),
    );
}

/// Push a prior function-call output, keyed back to the call name, onto the contents.
fn push_function_output(
    contents: &mut Vec<Content>,
    names: &HashMap<String, String>,
    call_id: &str,
    output: &str,
) {
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

/// Map each function-call `call_id` to its declared name across the input array.
fn collect_call_names(req: &CodexReq) -> HashMap<String, String> {
    let mut names: HashMap<String, String> = HashMap::new();
    for item in &req.input {
        if let CodexInput::FunctionCall { call_id, name, .. } = item {
            discard(names.insert(call_id.clone(), name.clone()));
        }
    }
    return names;
}

/// Capture a non-empty replayed reasoning signature as the pending signature.
fn capture_pending_sig(pending_sig: &mut Option<String>, encrypted_content: Option<&String>) {
    let Some(enc) = encrypted_content.filter(|enc| return !enc.is_empty()) else {
        return;
    };
    *pending_sig = Some(enc.clone());
}

/// Translate one codex input item into the gemini contents vector.
fn push_input_item(
    contents: &mut Vec<Content>,
    names: &HashMap<String, String>,
    pending_sig: &mut Option<String>,
    item: &CodexInput,
) {
    match item {
        CodexInput::Message { role, content } => {
            push_message(contents, role, content);
        },
        CodexInput::Reasoning { encrypted_content } => {
            capture_pending_sig(pending_sig, encrypted_content.as_ref());
        },
        CodexInput::FunctionCall {
            name, arguments, ..
        } => {
            push_function_call(contents, pending_sig, name, arguments);
        },
        CodexInput::FunctionCallOutput { call_id, output } => {
            push_function_output(contents, names, call_id, output);
        },
        CodexInput::Unknown => {},
    }
}

/// Reconstruct the gemini contents vector from the codex per-turn input array.
fn build_contents(req: &CodexReq) -> Vec<Content> {
    let names = collect_call_names(req);
    let mut contents: Vec<Content> = Vec::new();
    let mut pending_sig: Option<String> = None;
    for item in &req.input {
        push_input_item(&mut contents, &names, &mut pending_sig, item);
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
    meta: RespMeta<'_>,
) -> bool {
    state.seq = state.seq.wrapping_add(1);
    let created = ResponseStreamEvent::ResponseCreated(ResponseCreatedEvent {
        sequence_number: state.seq,
        response: make_response(meta, Status::InProgress, vec![], None),
    });
    if sender.send(Ok(to_event(&created))).await.is_err() {
        return false;
    }
    state.seq = state.seq.wrapping_add(1);
    let in_progress = ResponseStreamEvent::ResponseInProgress(ResponseInProgressEvent {
        sequence_number: state.seq,
        response: make_response(meta, Status::InProgress, vec![], None),
    });
    return sender.send(Ok(to_event(&in_progress))).await.is_ok();
}

/// Emit the reasoning item if pending; returns false if the receiver closed.
async fn flush_reasoning(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
) -> bool {
    if state.rsn_emitted == RsnEmitted::Yes
        || (state.reasoning.is_empty() && state.rsn_sig.is_empty())
    {
        return true;
    }
    state.rsn_emitted = RsnEmitted::Yes;
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

/// Open a fresh assistant message item and emit its `output_item.added`; false if receiver closed.
async fn open_message(sender: &Sender<Result<Event, Infallible>>, state: &mut StreamState) -> bool {
    state.msg_open = MsgOpen::Open;
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
    return sender.send(Ok(to_event(&added))).await.is_ok();
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
    if state.msg_open == MsgOpen::Closed && !open_message(sender, state).await {
        return false;
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

/// Queue a pending function call, capturing its signature; false if the receiver closed.
async fn handle_function_call(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    function_call: FunctionCall,
    thought_signature: Option<String>,
) -> bool {
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
        },
        Part::Text {
            text: part_text, ..
        } if !part_text.is_empty() => {
            return Box::pin(emit_text_part(sender, state, part_text)).await;
        },
        Part::FunctionCall {
            function_call,
            thought_signature,
        } => {
            return Box::pin(handle_function_call(
                sender,
                state,
                function_call,
                thought_signature,
            ))
            .await;
        },
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

/// Emit every part of one candidate, recording the finish reason; false if the receiver closed.
async fn handle_candidate(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    candidate: Candidate,
) -> bool {
    for part in candidate.content.parts.unwrap_or_default() {
        if !Box::pin(handle_part(sender, state, part)).await {
            return false;
        }
    }
    if let Some(finish_reason) = candidate.finish_reason {
        state.got_finish = FinishObserved::Yes;
        state.finish = Some(finish_reason);
    }
    return true;
}

/// Consume the gemini stream into the accumulator; returns false if the receiver closed.
async fn consume_stream(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    mut stream: GenerationStream,
) -> bool {
    while let Some(item) = stream.next().await {
        let Ok(chunk) = item else { break };
        if let Some(candidate) = chunk.candidates.into_iter().next()
            && !Box::pin(handle_candidate(sender, state, candidate)).await
        {
            return false;
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
    if state.msg_open == MsgOpen::Closed {
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
    let insert_at = if state.rsn_emitted == RsnEmitted::Yes {
        1_usize.min(state.out_items.len())
    } else {
        0_usize
    };
    state
        .out_items
        .insert(insert_at, OutputItem::Message(message));
    return true;
}

/// Build the four ordered SSE events one pending function call emits, bumping the sequence.
fn function_call_events(
    state: &mut StreamState,
    function_call: &FunctionToolCall,
    fc_id: &str,
    args: String,
) -> [ResponseStreamEvent; 4] {
    state.seq = state.seq.wrapping_add(1);
    let added = ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent {
        sequence_number: state.seq,
        output_index: state.output_index,
        item: OutputItem::FunctionCall(function_call.clone()),
    });
    state.seq = state.seq.wrapping_add(1);
    let delta = ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
        ResponseFunctionCallArgumentsDeltaEvent {
            sequence_number: state.seq,
            item_id: fc_id.to_owned(),
            output_index: state.output_index,
            delta: args.clone(),
        },
    );
    state.seq = state.seq.wrapping_add(1);
    let args_done = ResponseStreamEvent::ResponseFunctionCallArgumentsDone(
        ResponseFunctionCallArgumentsDoneEvent {
            name: None,
            sequence_number: state.seq,
            item_id: fc_id.to_owned(),
            output_index: state.output_index,
            arguments: args,
        },
    );
    state.seq = state.seq.wrapping_add(1);
    let item_done = ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
        sequence_number: state.seq,
        output_index: state.output_index,
        item: OutputItem::FunctionCall(function_call.clone()),
    });
    return [added, delta, args_done, item_done];
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
    for event in function_call_events(state, &function_call, &fc_id, args) {
        if sender.send(Ok(to_event(&event))).await.is_err() {
            return false;
        }
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
        },
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
    meta: RespMeta<'_>,
) {
    let out_items = take(&mut state.out_items);
    let usage = state.usage.take();
    if state.got_finish == FinishObserved::No {
        state.seq = state.seq.wrapping_add(1);
        let failed = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
            sequence_number: state.seq,
            response: make_response(meta, Status::Failed, out_items, usage),
        });
        discard(sender.send(Ok(to_event(&failed))).await);
        return;
    }
    if let Some(reason) = incomplete_reason(state.finish.as_ref()) {
        let mut resp = make_response(meta, Status::Incomplete, out_items, usage);
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
        response: make_response(meta, Status::Completed, out_items, usage),
    });
    discard(sender.send(Ok(to_event(&completed))).await);
}

/// Send a terminal `response.failed` carrying the given sequence number.
async fn send_failed(
    sender: &Sender<Result<Event, Infallible>>,
    meta: RespMeta<'_>,
    sequence_number: u64,
) {
    let failed = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
        sequence_number,
        response: make_response(meta, Status::Failed, vec![], None),
    });
    discard(sender.send(Ok(to_event(&failed))).await);
}

/// Drain a live gemini stream into the typed responses event sequence.
/// Emit every queued function call in order; returns false if the receiver closed.
async fn emit_function_calls(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
) -> bool {
    for (name, args) in take(&mut state.fcs) {
        if !Box::pin(emit_function_call(sender, state, name, args)).await {
            return false;
        }
    }
    return true;
}

/// Run the consume->flush->close->function-call stages; false if the receiver closed mid-stream.
async fn run_stream_stages(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    stream: GenerationStream,
) -> bool {
    return Box::pin(consume_stream(sender, state, stream)).await
        && flush_reasoning(sender, state).await
        && close_message(sender, state).await
        && emit_function_calls(sender, state).await;
}

/// Drive the gemini stream through every emit stage, then emit the terminal lifecycle event.
async fn drive_stream(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    stream: GenerationStream,
    meta: RespMeta<'_>,
) {
    if run_stream_stages(sender, state, stream).await {
        emit_terminal(sender, state, meta).await;
    }
}

/// Drive the gemini stream and emit the typed responses event sequence.
async fn stream_responses(
    builder: ContentBuilder,
    sender: Sender<Result<Event, Infallible>>,
    meta: RespMeta<'_>,
) {
    let mut state = StreamState::new();
    if !emit_open(&sender, &mut state, meta).await {
        return;
    }
    let Ok(stream) = Box::pin(builder.execute_stream()).await else {
        state.seq = state.seq.wrapping_add(1);
        send_failed(&sender, meta, state.seq).await;
        return;
    };
    Box::pin(drive_stream(&sender, &mut state, stream, meta)).await;
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
        discard(tokio::spawn(Box::pin(async move {
            let meta = RespMeta {
                model: &model,
                response_id: &response_id,
            };
            send_failed(&sender, meta, 1_u64).await;
        })));
        return Sse::new(ReceiverStream::new(receiver));
    };
    let builder = build_request(&client, &req, contents);

    discard(tokio::spawn(Box::pin(async move {
        let meta = RespMeta {
            model: &model,
            response_id: &response_id,
        };
        Box::pin(stream_responses(builder, sender, meta)).await;
    })));
    return Sse::new(ReceiverStream::new(receiver));
}

/// Entry point: bind the bridge and serve.
///
/// # Panics
///
/// Panics if the tokio runtime fails to build or the bound server task panics.
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
