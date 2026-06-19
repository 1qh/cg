//! MAX-TYPESAFE bridge: async-openai `CreateResponse` (typed request) -> gemini-rust (typed gemini)
//! -> async-openai `ResponseStreamEvent` (typed emit). No raw `serde_json::Value` for
//! request/response shapes.
use core::{convert::Infallible, iter::once, mem::take, time::Duration};
use std::{
    collections::{HashMap, HashSet},
    env::var,
    io::{Write as _, stderr},
    time::{SystemTime, UNIX_EPOCH},
};

use async_openai::types::responses::{
    AssistantRole, ErrorObject, FunctionToolCall, IncompleteDetails, InputTokenDetails, OutputItem,
    OutputMessage, OutputMessageContent, OutputStatus, OutputTextContent, OutputTokenDetails,
    ReasoningItem, Response, ResponseCompletedEvent, ResponseCreatedEvent, ResponseFailedEvent,
    ResponseFunctionCallArgumentsDeltaEvent, ResponseFunctionCallArgumentsDoneEvent,
    ResponseInProgressEvent, ResponseIncompleteEvent, ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent, ResponseStreamEvent, ResponseTextDeltaEvent, ResponseUsage,
    Status, SummaryPart, SummaryTextContent,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, Sse},
    routing::{get, post},
};
use futures::{StreamExt as _, stream::Stream};
use gemini_rust::{
    Blob, Candidate, Content, ContentBuilder, FinishReason, FunctionCall, FunctionCallingConfig,
    FunctionCallingMode, FunctionDeclaration, FunctionResponse, Gemini, GenerationResponse,
    GenerationStream, MediaResolution, MediaResolutionLevel, Model, Part, Role, ThinkingConfig,
    ThinkingLevel, Tool as GTool, UsageMetadata, tools::ToolConfig,
};
use serde::Deserialize;
use serde_json::Value;
use subtle::ConstantTimeEq as _;
use tokio::{
    net::TcpListener,
    sync::mpsc::{Sender, channel},
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Reasoning-effort level codex requests; a faithful mirror of codex's `ReasoningEffort`.
///
/// Every value codex can send maps to a level, and an unrecognized one falls to `Custom`, so no
/// effort value ever 422s the whole request. Kept faithful to codex by the source drift-check
/// (`adr/typed-domain.md`).
#[derive(Clone)]
enum CodexEffort {
    /// An effort value outside the known set; clamps to gemini `thinkingLevel` High.
    Custom,
    /// Maps to gemini `thinkingLevel` High.
    High,
    /// Maps to gemini `thinkingLevel` Low.
    Low,
    /// Maps to gemini `thinkingLevel` Medium.
    Medium,
    /// Maps to gemini `thinkingLevel` Minimal.
    Minimal,
    /// Codex's "none" (reasoning off); clamps to gemini `thinkingLevel` Minimal.
    None,
    /// Codex's xhigh; gemini rejects it, so it clamps to `thinkingLevel` High.
    Xhigh,
}
impl<'de> Deserialize<'de> for CodexEffort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = match String::deserialize(deserializer) {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        return Ok(match raw.as_str() {
            "high" => Self::High,
            "low" => Self::Low,
            "medium" => Self::Medium,
            "minimal" => Self::Minimal,
            "none" => Self::None,
            "xhigh" => Self::Xhigh,
            _ => Self::Custom,
        });
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Self::deserialize(deserializer) {
            Ok(value) => {
                *place = value;
                return Ok(());
            },
            Err(error) => return Err(error),
        }
    }
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
        /// Tool output payload: a plain string OR an array of structured content items.
        #[serde(default)]
        output: CodexOutput,
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
/// Caller tool-choice control codex sends as a mode string or a forced named-function object.
///
/// gemini's `FunctionCallingConfig` carries only a mode (no named-function forcing), so a named
/// object maps to `Any`. A manual deserialize accepts the string form, the object form, and any
/// other shape so no `tool_choice` value ever 422s the request.
enum CodexToolChoice {
    /// `auto` (or unrecognized): the model chooses; maps to gemini `Auto`.
    Auto,
    /// A forced named-function object `{ type, name }`; maps to gemini `Any`.
    Named,
    /// `none`: the model must not call tools; maps to gemini `None`.
    None,
    /// `required`: the model must call a tool; maps to gemini `Any`.
    Required,
}
impl<'de> Deserialize<'de> for CodexToolChoice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = match Value::deserialize(deserializer) {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        if raw.is_object() {
            return Ok(Self::Named);
        }
        return Ok(match raw.as_str() {
            Some("none") => Self::None,
            Some("required") => Self::Required,
            _ => Self::Auto,
        });
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Self::deserialize(deserializer) {
            Ok(value) => {
                *place = value;
                return Ok(());
            },
            Err(error) => return Err(error),
        }
    }
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
    /// Expected `Bearer` credential; `None` leaves the bridge open (documented localhost toggle).
    bridge_key: Option<String>,
}
/// One content part of a codex message; a faithful tagged mirror of codex's `ContentItem`.
///
/// Kept faithful to codex by the source drift-check (`adr/typed-domain.md`); an unrecognized kind
/// falls to `Unknown` so a new content kind never 422s the whole request.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexContent {
    /// An image part: a `data:<mime>;base64,<data>` URL; maps to a gemini inline-data part.
    InputImage {
        /// Caller-requested image resolution; maps to the gemini per-part `media_resolution`.
        #[serde(default)]
        detail: Option<CodexImageDetail>,
        /// The image data URL codex emits via `into_data_url()`.
        #[serde(default)]
        image_url: String,
    },
    /// A user/system/developer text part.
    InputText {
        /// Text body.
        #[serde(default)]
        text: String,
    },
    /// A replayed assistant text part.
    OutputText {
        /// Text body.
        #[serde(default)]
        text: String,
    },
    /// Any unrecognized content kind.
    #[serde(other)]
    Unknown,
}
/// Caller-requested image resolution; a faithful mirror of codex's `ImageDetail`.
///
/// Maps to gemini's per-part `media_resolution`; an unrecognized value falls to `Unknown` (model
/// default). Kept faithful to codex by the source drift-check.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum CodexImageDetail {
    /// Model-chosen resolution.
    Auto,
    /// High resolution (more tokens, higher quality).
    High,
    /// Low resolution (fewer tokens, lower quality).
    Low,
    /// Original resolution; maps to gemini High.
    Original,
    /// Any unrecognized detail value.
    #[serde(other)]
    Unknown,
}
/// A prior function-call output: a plain string OR structured content items.
///
/// A faithful mirror of codex's untagged `FunctionCallOutputBody`; modelling only the string form
/// (as `String`) 422s the WHOLE request when a tool returns structured/image content.
#[derive(Deserialize)]
#[serde(untagged)]
enum CodexOutput {
    /// The structured content-items output form.
    Items(Vec<CodexOutputItem>),
    /// The plain-string output form.
    Text(String),
}
impl Default for CodexOutput {
    fn default() -> Self {
        return Self::Text(String::new());
    }
}
/// One structured function-call output content item.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexOutputItem {
    /// Image output: a data/http URL.
    InputImage {
        /// The image URL.
        #[serde(default)]
        image_url: String,
    },
    /// Text output.
    InputText {
        /// Text body.
        #[serde(default)]
        text: String,
    },
    /// Any unrecognized output item kind.
    #[serde(other)]
    Unknown,
}
/// Reasoning control block.
#[derive(Deserialize)]
struct CodexReasoning {
    /// Requested reasoning effort.
    #[serde(default)]
    effort: Option<CodexEffort>,
}
/// Codex's response-format control: a JSON-schema for structured output, or plain text.
///
/// A faithful mirror of codex's `text.format` tagged union; `name`/`strict` are ignored (gemini
/// constrains on the schema alone), and any unrecognized format degrades to `Text`.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexTextFormat {
    /// Structured output constrained to a JSON schema.
    JsonSchema {
        /// The full JSON schema the output must conform to.
        schema: Value,
    },
    /// Plain text (or any unrecognized format).
    #[serde(other)]
    Text,
}
/// Codex's `text` control block carrying the response-format request.
#[derive(Deserialize)]
struct CodexText {
    /// The requested response format; absent leaves gemini's default free-form text.
    #[serde(default)]
    format: Option<CodexTextFormat>,
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
    /// Caller cap on output tokens; honored onto the gemini builder.
    #[serde(default)]
    max_output_tokens: Option<i32>,
    /// Requested model id, defaulting when absent.
    #[serde(default)]
    model: Option<String>,
    /// Reasoning-effort control.
    #[serde(default)]
    reasoning: Option<CodexReasoning>,
    /// Response-format control; a JSON-schema format constrains gemini's output.
    #[serde(default)]
    text: Option<CodexText>,
    /// Caller tool-choice control; maps to the gemini function-calling mode.
    #[serde(default)]
    tool_choice: Option<CodexToolChoice>,
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
    /// Nested function tools for a `namespace` group (multi-agent / MCP); empty for a leaf tool.
    #[serde(default)]
    tools: Vec<Self>,
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
/// The terminal lifecycle a gemini finish reason maps to.
enum TerminalOutcome {
    /// A clean stop; emit `response.completed`.
    Completed,
    /// An abnormal termination (malformed/excess tool calls, unsupported language, unspecified
    /// non-stop); emit `response.failed`.
    Failed,
    /// A bounded stop; emit `response.incomplete` with this reason.
    Incomplete(&'static str),
}
/// The outcome of awaiting the next gemini chunk under the per-chunk stall deadline.
enum NextChunk {
    /// A chunk arrived within the window.
    Chunk(Box<GenerationResponse>),
    /// The stream ended cleanly — drive the remaining stages.
    End,
    /// The stream yielded an error — emit `response.failed` carrying its message.
    Errored(String),
    /// No chunk arrived within the stall window — a mid-flight stall.
    Stalled,
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
    out_items: Vec<(u32, OutputItem)>,
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
    /// Unix seconds the request arrived, stamped as the response `created_at`.
    created: u64,
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
    let completed_at = if matches!(status, Status::InProgress) {
        None
    } else {
        Some(now_unix())
    };
    return Response {
        background: None,
        billing: None,
        conversation: None,
        created_at: meta.created,
        completed_at,
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
    let data = match serde_json::to_string(event) {
        Ok(json) => json,
        Err(error) => {
            discard(writeln!(stderr(), "event serialize failed: {error}"));
            String::new()
        },
    };
    return Event::default().data(data);
}

/// Parse a codex image-content `image_url` (`data:<mime>;base64,<data>`) into a gemini inline-data
/// blob; `None` when it is not a base64 data URL.
fn image_blob(image_url: &str) -> Option<Blob> {
    let Some(rest) = image_url.strip_prefix("data:") else {
        return None;
    };
    let Some((mime, b64)) = rest.split_once(";base64,") else {
        return None;
    };
    return Some(Blob::new(mime, b64));
}

/// Map codex's image `detail` to a gemini per-part media-resolution; `None` keeps the model
/// default.
const fn media_resolution(detail: Option<&CodexImageDetail>) -> Option<MediaResolution> {
    let Some(requested) = detail else {
        return None;
    };
    let level = match *requested {
        CodexImageDetail::Auto | CodexImageDetail::Unknown => {
            MediaResolutionLevel::MediaResolutionUnspecified
        },
        CodexImageDetail::High | CodexImageDetail::Original => {
            MediaResolutionLevel::MediaResolutionHigh
        },
        CodexImageDetail::Low => MediaResolutionLevel::MediaResolutionLow,
    };
    return Some(MediaResolution { level });
}

/// The gemini part one codex content item contributes (text, inline image, or nothing).
fn content_part(item: &CodexContent) -> Option<Part> {
    return match item {
        CodexContent::InputImage { detail, image_url } => {
            image_blob(image_url).map(|inline_data| {
                return Part::InlineData {
                    inline_data,
                    media_resolution: media_resolution(detail.as_ref()),
                };
            })
        },
        CodexContent::InputText { text } | CodexContent::OutputText { text } => {
            if text.is_empty() {
                None
            } else {
                Some(Part::Text {
                    text: text.clone(),
                    thought: None,
                    thought_signature: None,
                })
            }
        },
        CodexContent::Unknown => None,
    };
}

/// Build the gemini parts (text + inline image) one codex message's content contributes.
fn message_parts(content: &[CodexContent]) -> Vec<Part> {
    return content.iter().filter_map(content_part).collect();
}

/// Push the gemini content one codex message item contributes (text plus any inline image).
fn push_message(contents: &mut Vec<Content>, role: &CodexRole, content: &[CodexContent]) {
    let parts = message_parts(content);
    if parts.is_empty() {
        return;
    }
    let mapped_role = if *role == CodexRole::Assistant {
        Role::Model
    } else {
        Role::User
    };
    contents.push(Content {
        parts: Some(parts),
        role: Some(mapped_role),
    });
}

/// Push a prior function call (with its replayed thought signature) onto the contents.
fn push_function_call(
    contents: &mut Vec<Content>,
    pending_sig: Option<&String>,
    name: &str,
    arguments: &str,
) {
    let args: Value = match serde_json::from_str(arguments) {
        Ok(value) => value,
        Err(error) => {
            discard(writeln!(
                stderr(),
                "replayed function-call args parse failed: {error}"
            ));
            Value::Null
        },
    };
    let sig = pending_sig.map_or_else(
        || return "skip_thought_signature_validator".to_owned(),
        Clone::clone,
    );
    contents.push(
        Content::function_call_with_thought(FunctionCall::new(name, args), sig)
            .with_role(Role::Model),
    );
}

/// The text a single function-output item contributes; `None` for a non-text item.
fn output_item_text(item: &CodexOutputItem) -> Option<String> {
    return match item {
        CodexOutputItem::InputText { text } => Some(text.clone()),
        CodexOutputItem::InputImage { .. } | CodexOutputItem::Unknown => None,
    };
}

/// The gemini inline-data part a single function-output item contributes; `None` unless it is a
/// decodable image — so a tool image result becomes a part the model sees, never raw base64 text.
fn output_item_image(item: &CodexOutputItem) -> Option<Part> {
    let CodexOutputItem::InputImage { image_url } = item else {
        return None;
    };
    return image_blob(image_url).map(|inline_data| {
        return Part::InlineData {
            inline_data,
            media_resolution: None,
        };
    });
}

/// Split a function-call output (string or content items) into the flattened text the gemini
/// `function_response` carries plus any inline-image parts that ride a following user content.
fn output_parts(output: &CodexOutput) -> (String, Vec<Part>) {
    return match output {
        CodexOutput::Text(text) => (text.clone(), Vec::new()),
        CodexOutput::Items(items) => {
            let text = items
                .iter()
                .filter_map(output_item_text)
                .collect::<Vec<String>>()
                .join("\n");
            let images = items.iter().filter_map(output_item_image).collect();
            (text, images)
        },
    };
}

/// Push a prior function-call output, keyed back to the call name, onto the contents.
///
/// An orphan output (no matching call in this request) is skipped, never sent under a placeholder
/// name — gemini rejects a `function_response` whose name pairs with no functionCall. Image items
/// in the output ride a following user content as inline-data parts, never lost to text.
fn push_function_output(
    contents: &mut Vec<Content>,
    names: &HashMap<String, String>,
    call_id: &str,
    output: &CodexOutput,
) {
    let Some(name) = names.get(call_id) else {
        return;
    };
    let (text, images) = output_parts(output);
    contents.push(
        Content::function_response(FunctionResponse::new(
            name.clone(),
            serde_json::json!({ "output": text }),
        ))
        .with_role(Role::User),
    );
    if !images.is_empty() {
        contents.push(Content {
            parts: Some(images),
            role: Some(Role::User),
        });
    }
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
            *pending_sig = None;
            push_message(contents, role, content);
        },
        CodexInput::Reasoning { encrypted_content } => {
            capture_pending_sig(pending_sig, encrypted_content.as_ref());
        },
        CodexInput::FunctionCall {
            name, arguments, ..
        } => {
            push_function_call(contents, pending_sig.as_ref(), name, arguments);
        },
        CodexInput::FunctionCallOutput { call_id, output } => {
            *pending_sig = None;
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
const fn effort_level(effort: &CodexEffort) -> ThinkingLevel {
    return match effort {
        CodexEffort::Minimal | CodexEffort::None => ThinkingLevel::Minimal,
        CodexEffort::Low => ThinkingLevel::Low,
        CodexEffort::Medium => ThinkingLevel::Medium,
        CodexEffort::Custom | CodexEffort::High | CodexEffort::Xhigh => ThinkingLevel::High,
    };
}

/// Map codex's `tool_choice` to a gemini function-calling mode; `None` leaves the gemini default.
const fn tool_choice_mode(choice: Option<&CodexToolChoice>) -> Option<FunctionCallingMode> {
    let mode = match choice {
        None => return None,
        Some(CodexToolChoice::Auto) => FunctionCallingMode::Auto,
        Some(CodexToolChoice::Named | CodexToolChoice::Required) => FunctionCallingMode::Any,
        Some(CodexToolChoice::None) => FunctionCallingMode::None,
    };
    return Some(mode);
}

/// The gemini function-tool declaration for one codex tool; `None` for a non-function tool.
fn function_tool(tool: &CodexTool) -> Option<GTool> {
    if tool.kind != CodexToolKind::Function {
        return None;
    }
    let params = tool
        .parameters
        .clone()
        .unwrap_or_else(|| return serde_json::json!({ "type": "object", "properties": {} }));
    return Some(GTool::new(
        FunctionDeclaration::new(&tool.name, &tool.description, None)
            .with_parameters_json_schema_value(params),
    ));
}

/// Flatten codex's tool list to gemini function tools, descending into `namespace` groups, keeping
/// the first declaration of each function name.
///
/// codex groups multi-agent / MCP tools under a `namespace` entry whose `tools[]` hold the real
/// function declarations; a flat scan that only reads top-level `function` tools silently drops
/// them. The same function name can surface twice (a tool present both top-level and inside a
/// namespace, or two MCP servers exposing the same name) and gemini rejects the whole request with
/// `400 Duplicate function declaration` — so dedupe by name, first declaration wins.
fn function_tools(tools: &[CodexTool]) -> Vec<GTool> {
    let mut seen: HashSet<String> = HashSet::new();
    return tools
        .iter()
        .flat_map(|tool| return once(tool).chain(tool.tools.iter()))
        .filter(|tool| {
            return tool.kind == CodexToolKind::Function && seen.insert(tool.name.clone());
        })
        .filter_map(function_tool)
        .collect();
}

/// The gemini thinking config for the request's reasoning effort; thoughts always included.
///
/// Absent effort defaults to the catalog's declared `default_reasoning_level` (Medium), never
/// gemini's own built-in default, so the advertised default and the wire stay consistent.
fn thinking_config(req: &CodexReq) -> ThinkingConfig {
    let level = req
        .reasoning
        .as_ref()
        .and_then(|reasoning| return reasoning.effort.as_ref())
        .map_or(ThinkingLevel::Medium, effort_level);
    return ThinkingConfig::new()
        .with_thoughts_included(true)
        .with_thinking_level(level);
}

/// Build the gemini request builder from the codex request + reconstructed contents.
fn build_request(client: &Gemini, req: &CodexReq, contents: Vec<Content>) -> ContentBuilder {
    let mut builder = client.generate_content();
    builder.contents = contents;
    if let Some(instructions) = &req.instructions {
        builder = builder.with_system_prompt(instructions.clone());
    }
    if let Some(max_output_tokens) = req.max_output_tokens {
        builder = builder.with_max_output_tokens(max_output_tokens);
    }
    for tool in function_tools(&req.tools) {
        builder = builder.with_tool(tool);
    }
    builder = builder.with_tool(GTool::google_search());
    builder = builder.with_tool(GTool::url_context());
    builder = builder.with_tool(GTool::code_execution());
    builder = builder.with_tool_config(ToolConfig {
        function_calling_config: tool_choice_mode(req.tool_choice.as_ref())
            .map(|mode| return FunctionCallingConfig { mode }),
        include_server_side_tool_invocations: Some(true),
        ..ToolConfig::default()
    });
    builder = builder.with_thinking_config(thinking_config(req));
    let Some(schema) = response_schema(req) else {
        return builder;
    };
    return builder
        .with_response_mime_type("application/json")
        .with_response_json_schema(schema);
}

/// The JSON schema codex requests for structured output (`text.format` = `json_schema`), if any.
///
/// The bridge constrains gemini's output to it via `responseJsonSchema`, so a structured-output
/// request is honored rather than silently dropped to free-form text.
fn response_schema(req: &CodexReq) -> Option<Value> {
    let Some(text) = &req.text else {
        return None;
    };
    let Some(CodexTextFormat::JsonSchema { schema }) = &text.format else {
        return None;
    };
    return Some(schema.clone());
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
    state
        .out_items
        .push((state.output_index, OutputItem::Reasoning(reasoning_item)));
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
        if state.rsn_emitted == RsnEmitted::Yes {
            state.rsn_emitted = RsnEmitted::No;
            state.reasoning = String::new();
        }
        state.rsn_sig = signature;
    }
    if !flush_reasoning(sender, state).await {
        return false;
    }
    let arguments = fc_args_string(&function_call.args);
    state.fcs.push((function_call.name, arguments));
    return true;
}

/// Serialize gemini function-call args to a JSON object string; null/failure -> `{}` (codex needs
/// an object).
fn fc_args_string(args: &Value) -> String {
    if args.is_null() {
        return "{}".to_owned();
    }
    return match serde_json::to_string(args) {
        Ok(json) => json,
        Err(error) => {
            discard(writeln!(
                stderr(),
                "function-call args serialize failed: {error}"
            ));
            "{}".to_owned()
        },
    };
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
/// A gemini token count (`Option<i32>`) as a saturating `u32`.
fn token_count(value: Option<i32>) -> u32 {
    return u32::try_from(value.unwrap_or(0)).unwrap_or(0);
}

/// Map gemini usage onto codex `ResponseUsage` with `OpenAI` semantics.
///
/// gemini's `candidates_token_count` EXCLUDES thinking tokens (`total = prompt + candidates +
/// thoughts`), but `OpenAI`'s `output_tokens` INCLUDES `reasoning_tokens`; so output is
/// candidates+thoughts, keeping reasoning a subset of output and `input + output == total`.
fn map_usage(meta: &UsageMetadata) -> ResponseUsage {
    let reasoning_tokens = token_count(meta.thoughts_token_count);
    return ResponseUsage {
        input_tokens: token_count(meta.prompt_token_count),
        input_tokens_details: InputTokenDetails {
            cached_tokens: token_count(meta.cached_content_token_count),
        },
        output_tokens: token_count(meta.candidates_token_count).saturating_add(reasoning_tokens),
        output_tokens_details: OutputTokenDetails { reasoning_tokens },
        total_tokens: token_count(meta.total_token_count),
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

/// Fold one gemini chunk into the accumulator; returns false if the receiver closed.
async fn consume_chunk(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    chunk: GenerationResponse,
) -> bool {
    if let Some(candidate) = chunk.candidates.into_iter().next()
        && !Box::pin(handle_candidate(sender, state, candidate)).await
    {
        return false;
    }
    if let Some(meta) = &chunk.usage_metadata {
        state.usage = Some(map_usage(meta));
    }
    return true;
}

/// Await the next gemini chunk under a per-chunk stall deadline; a stall never false-kills a
/// healthy long stream the way a single total-drive cap does, and is caught promptly.
async fn next_chunk(stream: &mut GenerationStream) -> NextChunk {
    return match timeout(Duration::from_mins(2), Box::pin(stream.next())).await {
        Ok(Some(Ok(chunk))) => NextChunk::Chunk(Box::new(chunk)),
        Ok(Some(Err(error))) => {
            let message = format!("gemini stream error: {error}");
            discard(writeln!(stderr(), "{message}"));
            NextChunk::Errored(message)
        },
        Ok(None) => NextChunk::End,
        Err(_elapsed) => NextChunk::Stalled,
    };
}

/// Consume the gemini stream into the accumulator under the per-chunk stall deadline.
///
/// Emits `response.failed` and returns false on an inter-chunk stall or a closed receiver; returns
/// true when the stream ends cleanly so the caller drives the remaining emit stages.
async fn consume_stream(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    meta: RespMeta<'_>,
    mut stream: GenerationStream,
) -> bool {
    loop {
        let chunk = match next_chunk(&mut stream).await {
            NextChunk::Chunk(chunk) => chunk,
            NextChunk::End => return true,
            NextChunk::Errored(message) => {
                state.seq = state.seq.wrapping_add(1);
                send_failed(sender, meta, state.seq, message).await;
                return false;
            },
            NextChunk::Stalled => {
                discard(writeln!(stderr(), "gemini stream stall timeout 120s"));
                Box::pin(emit_terminal(sender, state, meta)).await;
                return false;
            },
        };
        if !Box::pin(consume_chunk(sender, state, *chunk)).await {
            return false;
        }
    }
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
    state
        .out_items
        .push((state.msg_oi, OutputItem::Message(message)));
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
        .push((state.output_index, OutputItem::FunctionCall(function_call)));
    state.output_index = state.output_index.wrapping_add(1);
    return true;
}

/// Map a gemini finish reason onto its responses terminal lifecycle.
///
/// A non-STOP finish must NEVER report `Completed`: a content block reports `content_filter`, a
/// length cap reports `max_output_tokens`, and any other abnormal stop reports `Failed`. The match
/// is exhaustive, so a new gemini variant breaks the build until it is categorized here.
const fn finish_outcome(finish: Option<&FinishReason>) -> TerminalOutcome {
    return match finish {
        None | Some(&(FinishReason::FinishReasonUnspecified | FinishReason::Stop)) => {
            TerminalOutcome::Completed
        },
        Some(&FinishReason::MaxTokens) => TerminalOutcome::Incomplete("max_output_tokens"),
        Some(
            &(FinishReason::Blocklist
            | FinishReason::ImageSafety
            | FinishReason::ProhibitedContent
            | FinishReason::Recitation
            | FinishReason::Safety
            | FinishReason::Spii),
        ) => TerminalOutcome::Incomplete("content_filter"),
        Some(
            &(FinishReason::Language
            | FinishReason::MalformedFunctionCall
            | FinishReason::Other
            | FinishReason::TooManyToolCalls
            | FinishReason::UnexpectedToolCall),
        ) => TerminalOutcome::Failed,
    };
}

/// Emit the terminal lifecycle event based on finish state.
async fn emit_terminal(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    meta: RespMeta<'_>,
) {
    let mut tagged = take(&mut state.out_items);
    tagged.sort_by_key(|entry| return entry.0);
    let out_items: Vec<OutputItem> = tagged.into_iter().map(|entry| return entry.1).collect();
    let usage = state.usage.take();
    let outcome = if state.got_finish == FinishObserved::No {
        TerminalOutcome::Failed
    } else {
        finish_outcome(state.finish.as_ref())
    };
    state.seq = state.seq.wrapping_add(1);
    let event = match outcome {
        TerminalOutcome::Completed => {
            ResponseStreamEvent::ResponseCompleted(ResponseCompletedEvent {
                sequence_number: state.seq,
                response: make_response(meta, Status::Completed, out_items, usage),
            })
        },
        TerminalOutcome::Failed => failed_event(
            state.seq,
            make_response(meta, Status::Failed, out_items, usage),
            state.finish.as_ref(),
        ),
        TerminalOutcome::Incomplete(reason) => {
            let mut resp = make_response(meta, Status::Incomplete, out_items, usage);
            resp.incomplete_details = Some(IncompleteDetails {
                reason: reason.into(),
            });
            ResponseStreamEvent::ResponseIncomplete(ResponseIncompleteEvent {
                sequence_number: state.seq,
                response: resp,
            })
        },
    };
    discard(sender.send(Ok(to_event(&event))).await);
}

/// A human-readable failure message for a terminal `response.failed`, derived from the gemini
/// finish reason (or the no-finish case), so codex surfaces a cause rather than an empty error.
fn failed_message(finish: Option<&FinishReason>) -> String {
    return finish.map_or_else(
        || return "gemini stream ended without a finish reason".to_owned(),
        |reason| return format!("gemini terminated abnormally: {reason:?}"),
    );
}

/// Wrap a failed `Response` into a `response.failed` event with a populated error cause so codex
/// never receives a failed response with an empty error.
fn failed_event(
    sequence_number: u64,
    mut response: Response,
    finish: Option<&FinishReason>,
) -> ResponseStreamEvent {
    response.error = Some(ErrorObject {
        code: "upstream_failure".to_owned(),
        message: failed_message(finish),
    });
    return ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
        sequence_number,
        response,
    });
}

/// Send a terminal `response.failed` carrying the given sequence number and a populated error so
/// codex surfaces a cause; used for pre-stream rejections (no model, connect failure).
async fn send_failed(
    sender: &Sender<Result<Event, Infallible>>,
    meta: RespMeta<'_>,
    sequence_number: u64,
    message: String,
) {
    let mut response = make_response(meta, Status::Failed, vec![], None);
    response.error = Some(ErrorObject {
        code: "upstream_failure".to_owned(),
        message,
    });
    let failed = ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
        sequence_number,
        response,
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
    meta: RespMeta<'_>,
) -> bool {
    return Box::pin(consume_stream(sender, state, meta, stream)).await
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
    if Box::pin(run_stream_stages(sender, state, stream, meta)).await {
        emit_terminal(sender, state, meta).await;
    }
}

/// Emit `response.failed` for a stream that never opened; returns `None` to short-circuit the
/// caller.
async fn fail_stream(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    meta: RespMeta<'_>,
    message: String,
) -> Option<GenerationStream> {
    state.seq = state.seq.wrapping_add(1);
    send_failed(sender, meta, state.seq, message).await;
    return None;
}

/// Establish the gemini stream under a connect deadline, surfacing the specific failure reason.
async fn open_gemini_stream(
    builder: ContentBuilder,
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    meta: RespMeta<'_>,
) -> Option<GenerationStream> {
    let connect = timeout(Duration::from_mins(1), Box::pin(builder.execute_stream()));
    let stream = match connect.await {
        Ok(Ok(opened)) => opened,
        Ok(Err(error)) => {
            let message = format!("gemini connect error: {error}");
            discard(writeln!(stderr(), "{message}"));
            return fail_stream(sender, state, meta, message).await;
        },
        Err(_elapsed) => {
            let message = "gemini connect timeout 60s".to_owned();
            discard(writeln!(stderr(), "{message}"));
            return fail_stream(sender, state, meta, message).await;
        },
    };
    return Some(stream);
}

/// Drive the established stream to its terminal event.
///
/// The per-chunk stall deadline inside `consume_stream` bounds every stream await, so a mid-stream
/// stall emits `response.failed` without a total-drive cap that would false-kill a healthy long
/// stream.
async fn drive_with_deadline(
    sender: &Sender<Result<Event, Infallible>>,
    state: &mut StreamState,
    stream: GenerationStream,
    meta: RespMeta<'_>,
) {
    Box::pin(drive_stream(sender, state, stream, meta)).await;
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
    let Some(stream) = Box::pin(open_gemini_stream(builder, &sender, &mut state, meta)).await
    else {
        return;
    };
    drive_with_deadline(&sender, &mut state, stream, meta).await;
}

/// Unix seconds now; `0` if the clock is before the epoch.
fn now_unix() -> u64 {
    return SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| return elapsed.as_secs());
}

/// Spawn a task emitting a single `response.failed` for a request rejected before streaming starts.
///
/// The specific cause is logged to stderr at the call site; codex receives a populated non-empty
/// error so it never sees an empty failure.
fn spawn_failed(
    sender: Sender<Result<Event, Infallible>>,
    response_id: String,
    model: String,
    created: u64,
) {
    discard(tokio::spawn(Box::pin(async move {
        let meta = RespMeta {
            created,
            model: &model,
            response_id: &response_id,
        };
        send_failed(
            &sender,
            meta,
            1_u64,
            "request rejected before streaming".to_owned(),
        )
        .await;
    })));
}

/// Codex `/v1/responses` handler: translate to gemini, stream typed responses events.
/// Whether the request carries the configured `Bearer` credential; open when no key is configured.
fn authorized(headers: &HeaderMap, bridge_key: Option<&String>) -> bool {
    let Some(expected) = bridge_key else {
        return true;
    };
    let Some(header) = headers.get("authorization") else {
        return false;
    };
    let Ok(value) = header.to_str() else {
        return false;
    };
    let expected_header = format!("Bearer {expected}");
    return value.as_bytes().ct_eq(expected_header.as_bytes()).into();
}

/// Resolve the model + gemini client, then spawn the streaming task; emit `response.failed` on a
/// missing model or client-construction failure.
fn launch_stream(api_key: String, req: &CodexReq, sender: Sender<Result<Event, Infallible>>) {
    let created = now_unix();
    let response_id = format!("resp_{}", Uuid::new_v4().simple());
    let Some(model) = req
        .model
        .clone()
        .filter(|requested| return !requested.is_empty())
    else {
        discard(writeln!(
            stderr(),
            "request rejected: no model (no fallback)"
        ));
        discard(api_key);
        spawn_failed(sender, response_id, String::new(), created);
        return;
    };
    let contents = build_contents(req);
    let api_model = if model.starts_with("models/") {
        model.clone()
    } else {
        format!("models/{model}")
    };
    let Ok(client) = Gemini::with_model(api_key, Model::Custom(api_model)) else {
        spawn_failed(sender, response_id, model, created);
        return;
    };
    let builder = build_request(&client, req, contents);
    discard(tokio::spawn(Box::pin(async move {
        let meta = RespMeta {
            created,
            model: &model,
            response_id: &response_id,
        };
        Box::pin(stream_responses(builder, sender, meta)).await;
    })));
}

/// Codex `/v1/responses` handler: translate to gemini, stream typed responses events.
///
/// # Errors
/// Returns `401 Unauthorized` when a configured `BRIDGE_KEY` does not match the request `Bearer`.
async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CodexReq>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    if !authorized(&headers, state.bridge_key.as_ref()) {
        discard(writeln!(
            stderr(),
            "request rejected: bad bridge credential"
        ));
        return Err(StatusCode::UNAUTHORIZED);
    }
    let (sender, receiver) = channel::<Result<Event, Infallible>>(64);
    launch_stream(state.api_key, &req, sender);
    return Ok(Sse::new(ReceiverStream::new(receiver)));
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
    let bridge_key = var("BRIDGE_KEY").ok().filter(|key| return !key.is_empty());
    if bridge_key.is_none() {
        discard(writeln!(
            stderr(),
            "BRIDGE_KEY unset: bridge open on loopback (set BRIDGE_KEY to require a Bearer \
             credential)"
        ));
    }
    let app = Router::new()
        .route("/v1/responses", post(responses))
        .route("/health/liveliness", get(async || return "ok"))
        .layer(DefaultBodyLimit::max(0x400_0000))
        .with_state(AppState {
            api_key,
            bridge_key,
        });
    let Ok(port) = var("PORT") else {
        discard(writeln!(stderr(), "PORT env required (no fallback)"));
        return;
    };
    let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{port}")).await else {
        discard(writeln!(stderr(), "bind failed on :{port}"));
        return;
    };
    discard(writeln!(stderr(), "typed bridge on :{port}"));
    if let Err(err) = axum::serve(listener, app).await {
        discard(writeln!(stderr(), "serve failed: {err}"));
    }
}
