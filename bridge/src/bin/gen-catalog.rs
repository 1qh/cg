//! Generate `gemini-catalog.json` from typed model metadata + the shared base-instructions source.
//!
//! The catalog is codegen output, never hand-edited (`adr/typed-domain.md`): one typed template +
//! the per-tier (slug, `display_name`, description) trio, so a field typo is a compile error and
//! the 21KB base-instructions prompt lives once (not duplicated per model). Run via `make catalog`;
//! `make catalog-check` regenerates and diffs to fail on drift or a hand-edit.
use std::{
    fs::write,
    io::{Write as _, stderr},
    process::ExitCode,
};

use async_openai as _;
use axum as _;
use futures as _;
use gemini_rust as _;
use serde::Serialize;
use subtle as _;
use tokio as _;
use tokio_stream as _;
use uuid as _;

/// The engine's full coding-agent prompt, shared by every tier.
const BASE_INSTRUCTIONS: &str = include_str!("../../base-instructions.txt");

/// A reasoning-effort option surfaced for a model.
#[derive(Serialize)]
struct ReasoningLevel {
    /// Effort description.
    description: &'static str,
    /// Effort key.
    effort: &'static str,
}

/// The byte-truncation policy for tool output.
#[derive(Serialize)]
struct TruncationPolicy {
    /// Byte cap.
    limit: u32,
    /// Truncation mode.
    mode: &'static str,
}

/// One catalog model entry; fields are alphabetical to match the codegen lint + canonical JSON.
///
/// reason: a wire DTO mirroring codex's catalog schema, whose `supports_*`/`support_*` fields are
/// independent JSON booleans; modelling them as enums would break the wire contract.
#[expect(
    clippy::struct_excessive_bools,
    reason = "codex catalog schema requires independent bool fields"
)]
#[derive(Serialize)]
struct CatalogModel {
    /// `apply_patch` tool type; null for gemini (edits via shell).
    apply_patch_tool_type: Option<&'static str>,
    /// Engine auto-compaction trigger; null (app-layer compaction owns this).
    auto_compact_token_limit: Option<u32>,
    /// The shared coding-agent prompt.
    base_instructions: &'static str,
    /// Real model context window.
    context_window: u32,
    /// Default reasoning effort.
    default_reasoning_level: &'static str,
    /// Default reasoning summary mode.
    default_reasoning_summary: &'static str,
    /// Default verbosity.
    default_verbosity: &'static str,
    /// Per-tier description.
    description: &'static str,
    /// Per-tier display name.
    display_name: &'static str,
    /// Usable context-window percentage.
    effective_context_window_percent: u32,
    /// Experimental tool names.
    experimental_supported_tools: Vec<&'static str>,
    /// Max context window.
    max_context_window: u32,
    /// Max output tokens.
    max_output_tokens: u32,
    /// Multi-agent protocol version; null.
    multi_agent_version: Option<&'static str>,
    /// Catalog ordering priority.
    priority: u32,
    /// Shell tool type.
    shell_type: &'static str,
    /// Model id.
    slug: &'static str,
    /// Whether verbosity control is supported.
    support_verbosity: bool,
    /// Whether the model is exposed in the API.
    supported_in_api: bool,
    /// Reasoning-effort options.
    supported_reasoning_levels: Vec<ReasoningLevel>,
    /// Whether image detail `original` is supported.
    supports_image_detail_original: bool,
    /// Whether parallel tool calls are supported.
    supports_parallel_tool_calls: bool,
    /// Whether reasoning summaries are supported.
    supports_reasoning_summaries: bool,
    /// Whether the search tool is supported.
    supports_search_tool: bool,
    /// Tool mode.
    tool_mode: &'static str,
    /// Byte-truncation policy for tool output.
    truncation_policy: TruncationPolicy,
    /// Catalog visibility.
    visibility: &'static str,
    /// Web-search tool type.
    web_search_tool_type: &'static str,
}

/// The full catalog document.
#[derive(Serialize)]
struct Catalog {
    /// The model entries.
    models: Vec<CatalogModel>,
}

/// Discard a value whose result is intentionally unused.
fn discard<T>(_value: T) {}

/// The reasoning-effort options every tier shares.
fn reasoning_levels() -> Vec<ReasoningLevel> {
    return vec![
        ReasoningLevel {
            description: "Fastest, lightest reasoning for simple tasks where depth is unneeded",
            effort: "minimal",
        },
        ReasoningLevel {
            description: "Balances speed with some reasoning; useful for straightforward queries \
                          and short explanations",
            effort: "low",
        },
        ReasoningLevel {
            description: "Provides a solid balance of reasoning depth and latency for \
                          general-purpose tasks",
            effort: "medium",
        },
        ReasoningLevel {
            description: "Maximizes reasoning depth for complex or ambiguous problems",
            effort: "high",
        },
        ReasoningLevel {
            description: "Extra high reasoning for complex problems",
            effort: "xhigh",
        },
    ];
}

/// Build one model entry from its per-tier identity; every other field is the shared template.
fn model(
    slug: &'static str,
    display_name: &'static str,
    description: &'static str,
) -> CatalogModel {
    return CatalogModel {
        apply_patch_tool_type: None,
        auto_compact_token_limit: None,
        base_instructions: BASE_INSTRUCTIONS,
        context_window: 0x10_0000,
        default_reasoning_level: "medium",
        default_reasoning_summary: "auto",
        default_verbosity: "low",
        description,
        display_name,
        effective_context_window_percent: 90,
        experimental_supported_tools: vec![],
        max_context_window: 0x10_0000,
        max_output_tokens: 0x1_0000,
        multi_agent_version: None,
        priority: 10,
        shell_type: "shell_command",
        slug,
        support_verbosity: true,
        supported_in_api: true,
        supported_reasoning_levels: reasoning_levels(),
        supports_image_detail_original: true,
        supports_parallel_tool_calls: true,
        supports_reasoning_summaries: true,
        supports_search_tool: true,
        tool_mode: "default",
        truncation_policy: TruncationPolicy {
            limit: 10_000,
            mode: "bytes",
        },
        visibility: "list",
        web_search_tool_type: "text",
    };
}

/// Generate the catalog JSON and write it to `gemini-catalog.json` in the working directory.
fn main() -> ExitCode {
    let catalog = Catalog {
        models: vec![
            model(
                "gemini-3.1-pro-preview",
                "Gemini 3.1 Pro",
                "Gemini 3.1 Pro via pure-Rust bridge",
            ),
            model(
                "gemini-3.5-flash",
                "Gemini 3.5 Flash",
                "Gemini 3.5 Flash via pure-Rust bridge",
            ),
            model(
                "gemini-3.1-flash-lite",
                "Gemini 3.1 Flash-Lite",
                "Gemini 3.1 Flash-Lite via pure-Rust bridge",
            ),
        ],
    };
    let json = match serde_json::to_string_pretty(&catalog) {
        Ok(value) => value,
        Err(_error) => {
            discard(writeln!(stderr(), "catalog serialize failed"));
            return ExitCode::FAILURE;
        },
    };
    match write("gemini-catalog.json", format!("{json}\n")) {
        Ok(()) => {
            discard(writeln!(stderr(), "ok"));
            return ExitCode::SUCCESS;
        },
        Err(_error) => {
            discard(writeln!(stderr(), "catalog write failed"));
            return ExitCode::FAILURE;
        },
    }
}
