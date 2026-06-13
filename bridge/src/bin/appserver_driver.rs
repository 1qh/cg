//! SPIKE: can PURE RUST drive codex `app-server` over JSON-RPC (the verify-harness side)?
//! Spawn codex `app-server` -> `initialize` -> `thread/start` -> `turn/start` -> capture agent message + completion.
//! If this works, the whole product (bridge + launcher + can-fail suite) can be pure Rust.
use async_openai as _;
use axum as _;
use futures as _;
use gemini_rust as _;
use serde as _;
use serde_json::{Value, json};
use std::env::{temp_dir, var};
use std::fs::create_dir_all;
use std::io::Error as IoError;
use std::io::Write as _;
use std::io::{stderr, stdout};
use std::path::Path;
use std::process::id as process_id;
use std::process::{ExitCode, Stdio};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_stream as _;
use uuid as _;

/// Outcome of driving the app-server: whether the turn finished and the captured reply.
struct DriveOutcome {
    /// Whether a `turn/completed` or `turn/failed` notification arrived.
    done: bool,
    /// The captured agent message text.
    msg: String,
}

/// Consume a value, suppressing the result. Why: kills `let_underscore` + `unused_results` lints.
fn discard<T>(_value: T) {}

/// Drive the JSON-RPC loop: reply to server requests, start a thread, start a turn, capture the message.
async fn drive(
    stdin: &mut ChildStdin,
    lines: &mut Lines<BufReader<ChildStdout>>,
    work_dir: &Path,
    counter: &mut u64,
) -> DriveOutcome {
    let mut thread_id = String::new();
    let mut msg = String::new();
    let mut done = false;
    let mut started = false;
    while let Ok(Some(rawline)) = lines.next_line().await {
        let trimmed = rawline.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if parsed.get("id").is_some() && parsed.get("method").is_some() {
            let reply = format!("{}\n", json!({"id": parsed.get("id"), "result": {}}));
            let Ok(()) = stdin.write_all(reply.as_bytes()).await else {
                return DriveOutcome { done, msg };
            };
            continue;
        }
        if parsed.get("id").is_some() && parsed.get("result").is_some() && !started {
            match thread_id.as_str() {
                "" => {
                    let cwd = work_dir.to_str().unwrap_or("");
                    let request = rpc_line(
                        "thread/start",
                        &json!({"model":"gemini-3.5-flash","modelProvider":"r","cwd":cwd,"approvalPolicy":"never","sandbox":"read-only"}),
                        counter,
                    );
                    let Ok(()) = stdin.write_all(request.as_bytes()).await else {
                        return DriveOutcome { done, msg };
                    };
                    thread_id = "pending".into();
                }
                "pending" => {
                    let tid = parsed
                        .pointer("/result/thread/id")
                        .and_then(Value::as_str)
                        .or_else(|| {
                            return parsed.pointer("/result/threadId").and_then(Value::as_str);
                        })
                        .unwrap_or("")
                        .to_owned();
                    thread_id = tid.clone();
                    let request = rpc_line(
                        "turn/start",
                        &json!({"threadId":tid,"input":[{"type":"text","text":"Reply with exactly: PURE_RUST_DRIVER_OK","text_elements":[]}]}),
                        counter,
                    );
                    let Ok(()) = stdin.write_all(request.as_bytes()).await else {
                        return DriveOutcome { done, msg };
                    };
                    started = true;
                }
                _ => {}
            }
        }
        if let Some(method) = parsed.get("method").and_then(Value::as_str) {
            if let Some(item) = parsed.pointer("/params/item")
                && item.get("type").and_then(Value::as_str) == Some("agent_message")
                && let Some(text) = item.get("text").and_then(Value::as_str)
            {
                msg = text.to_owned();
            }
            if method == "item/agentMessage/delta"
                && let Some(delta) = parsed.pointer("/params/delta").and_then(Value::as_str)
            {
                msg.push_str(delta);
            }
            if method == "turn/completed" || method == "turn/failed" {
                done = true;
            }
        }
        if done {
            break;
        }
    }
    return DriveOutcome { done, msg };
}

/// Initialize a throwaway git repo in `work_dir` so codex treats it as a project root.
async fn init_repo(work_dir: &Path) {
    discard(
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(work_dir)
            .status()
            .await,
    );
    discard(
        Command::new("git")
            .args(["commit", "-q", "--allow-empty", "-m", "i"])
            .current_dir(work_dir)
            .env("GIT_AUTHOR_NAME", "x")
            .env("GIT_AUTHOR_EMAIL", "a@b.c")
            .env("GIT_COMMITTER_NAME", "x")
            .env("GIT_COMMITTER_EMAIL", "a@b.c")
            .status()
            .await,
    );
}

/// Build one JSON-RPC request line and advance the request id counter.
fn rpc_line(method: &str, params: &Value, counter: &mut u64) -> String {
    let line = format!(
        "{}\n",
        json!({"method": method, "id": *counter, "params": params})
    );
    *counter = (*counter).wrapping_add(1);
    return line;
}

/// Run the spike: spawn codex app-server, drive it, print the proof line; `Err` signals a setup failure.
async fn run() -> Result<(), ()> {
    let work_dir = temp_dir().join(format!("rsdrv-{}", process_id()));
    let Ok(()) = create_dir_all(&work_dir) else {
        return Err(());
    };
    init_repo(&work_dir).await;

    let Ok(port) = var("BRIDGE_PORT") else {
        discard(writeln!(stderr(), "BRIDGE_PORT env required (no fallback)"));
        return Err(());
    };
    let Ok(mut child) = spawn_codex(&port) else {
        return Err(());
    };
    let Some(mut stdin) = child.stdin.take() else {
        return Err(());
    };
    let Some(child_stdout) = child.stdout.take() else {
        return Err(());
    };
    let mut lines = BufReader::new(child_stdout).lines();
    let mut counter = 1_u64;
    let init = rpc_line(
        "initialize",
        &json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}),
        &mut counter,
    );
    let Ok(()) = stdin.write_all(init.as_bytes()).await else {
        return Err(());
    };

    let outcome = drive(&mut stdin, &mut lines, &work_dir, &mut counter).await;
    discard(child.kill().await);
    let done = outcome.done;
    let msg = outcome.msg;
    discard(writeln!(
        stdout(),
        "  pure-Rust driver: done={done} reply={msg}"
    ));
    discard(writeln!(
        stdout(),
        "  >> {}",
        if done && msg.contains("PURE_RUST_DRIVER_OK") {
            "PROVEN \u{2014} pure Rust drives codex app-server e2e (verify harnesses can be Rust)"
        } else {
            "see above"
        }
    ));
    return Ok(());
}

/// Spawn `codex app-server` wired to the BYOK provider on the bridge port.
fn spawn_codex(port: &str) -> Result<Child, IoError> {
    let base_url = format!("model_providers.r.base_url=\"http://localhost:{port}/v1\"");
    return Command::new("codex")
        .args([
            "app-server",
            "-c",
            "model_provider=r",
            "-c",
            "model_providers.r.name=\"r\"",
            "-c",
            &base_url,
            "-c",
            "model_providers.r.wire_api=\"responses\"",
            "-c",
            "model_providers.r.env_key=\"K\"",
        ])
        .env("K", "x")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn();
}

/// Spike entry point: delegate to `run`, mapping a setup failure to a non-zero exit code.
#[tokio::main]
async fn main() -> ExitCode {
    return match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => ExitCode::FAILURE,
    };
}
