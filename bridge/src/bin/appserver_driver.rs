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

/// Where the thread/turn handshake stands.
#[derive(PartialEq, Eq)]
enum Phase {
    /// No `thread/start` issued yet.
    Idle,
    /// `thread/start` issued, awaiting its result.
    Pending,
    /// `turn/start` issued.
    Started,
}

/// Outcome of driving the app-server: whether the turn finished and the captured reply.
struct DriveOutcome {
    /// Whether a `turn/completed` or `turn/failed` notification arrived.
    done: bool,
    /// The captured agent message text.
    msg: String,
}

/// Mutable state threaded through the JSON-RPC drive loop.
struct DriveState {
    /// Whether a `turn/completed` or `turn/failed` notification arrived.
    done: bool,
    /// The captured agent message text.
    msg: String,
    /// How far the thread/turn handshake has progressed.
    phase: Phase,
}

/// Loop-constant connection: the codex stdin, the work dir, and the request-id counter.
struct Conn<'conn> {
    /// The monotonically advancing JSON-RPC request id.
    counter: &'conn mut u64,
    /// The codex app-server stdin to write requests into.
    stdin: &'conn mut ChildStdin,
    /// The project root passed to `thread/start`.
    work_dir: &'conn Path,
}

/// Consume a value, suppressing the result. Why: kills `let_underscore` + `unused_results` lints.
fn discard<T>(_value: T) {}

/// Issue `thread/start`, marking the thread id pending.
///
/// # Errors
/// Returns `Err(())` when writing the request to codex stdin fails.
async fn send_thread_start(conn: &mut Conn<'_>, state: &mut DriveState) -> Result<(), ()> {
    let cwd = conn.work_dir.to_str().unwrap_or("");
    let request = rpc_line(
        "thread/start",
        &json!({"model":"gemini-3.5-flash","modelProvider":"r","cwd":cwd,"approvalPolicy":"never","sandbox":"read-only"}),
        conn.counter,
    );
    let Ok(()) = conn.stdin.write_all(request.as_bytes()).await else {
        return Err(());
    };
    state.phase = Phase::Pending;
    return Ok(());
}

/// Read the thread id from the result and issue `turn/start`.
///
/// # Errors
/// Returns `Err(())` when writing the request to codex stdin fails.
async fn send_turn_start(
    conn: &mut Conn<'_>,
    state: &mut DriveState,
    parsed: &Value,
) -> Result<(), ()> {
    let tid = parsed
        .pointer("/result/thread/id")
        .and_then(Value::as_str)
        .or_else(|| return parsed.pointer("/result/threadId").and_then(Value::as_str))
        .unwrap_or("")
        .to_owned();
    let request = rpc_line(
        "turn/start",
        &json!({"threadId":tid,"input":[{"type":"text","text":"Reply with exactly: PURE_RUST_DRIVER_OK","text_elements":[]}]}),
        conn.counter,
    );
    let Ok(()) = conn.stdin.write_all(request.as_bytes()).await else {
        return Err(());
    };
    state.phase = Phase::Started;
    return Ok(());
}

/// Advance the thread/turn handshake on a result line.
///
/// # Errors
/// Returns `Err(())` when writing a follow-up request to codex stdin fails.
async fn handle_result(
    conn: &mut Conn<'_>,
    state: &mut DriveState,
    parsed: &Value,
) -> Result<(), ()> {
    if state.phase == Phase::Started || parsed.get("id").is_none() || parsed.get("result").is_none()
    {
        return Ok(());
    }
    if state.phase == Phase::Idle {
        return send_thread_start(conn, state).await;
    }
    if state.phase == Phase::Pending {
        return send_turn_start(conn, state, parsed).await;
    }
    return Ok(());
}

/// Capture the agent message and completion flag from a notification line.
fn handle_notification(state: &mut DriveState, parsed: &Value) {
    let Some(method) = parsed.get("method").and_then(Value::as_str) else {
        return;
    };
    if let Some(item) = parsed.pointer("/params/item")
        && item.get("type").and_then(Value::as_str) == Some("agent_message")
        && let Some(text) = item.get("text").and_then(Value::as_str)
    {
        state.msg.clear();
        state.msg.push_str(text);
    }
    if method == "item/agentMessage/delta"
        && let Some(delta) = parsed.pointer("/params/delta").and_then(Value::as_str)
    {
        state.msg.push_str(delta);
    }
    if method == "turn/completed" || method == "turn/failed" {
        state.done = true;
    }
}

/// Reply to a server->client request line; returns `true` when the line was such a request.
///
/// # Errors
/// Returns `Err(())` when writing the reply to codex stdin fails.
async fn reply_to_server_request(conn: &mut Conn<'_>, parsed: &Value) -> Result<bool, ()> {
    if parsed.get("id").is_none() || parsed.get("method").is_none() {
        return Ok(false);
    }
    let reply = format!("{}\n", json!({"id": parsed.get("id"), "result": {}}));
    let Ok(()) = conn.stdin.write_all(reply.as_bytes()).await else {
        return Err(());
    };
    return Ok(true);
}

/// Process one raw input line.
///
/// # Errors
/// Returns `Err(())` on a write failure that ends the drive loop.
async fn drive_line(conn: &mut Conn<'_>, state: &mut DriveState, rawline: &str) -> Result<(), ()> {
    let trimmed = rawline.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    let Ok(parsed) = serde_json::from_str::<Value>(trimmed) else {
        return Ok(());
    };
    let Ok(replied) = reply_to_server_request(conn, &parsed).await else {
        return Err(());
    };
    if replied {
        return Ok(());
    }
    let Ok(()) = handle_result(conn, state, &parsed).await else {
        return Err(());
    };
    handle_notification(state, &parsed);
    return Ok(());
}

/// Drive the JSON-RPC loop: reply to server requests, start a thread, start a turn, capture the message.
async fn drive(
    stdin: &mut ChildStdin,
    lines: &mut Lines<BufReader<ChildStdout>>,
    work_dir: &Path,
    counter: &mut u64,
) -> DriveOutcome {
    let mut state = DriveState {
        phase: Phase::Idle,
        msg: String::new(),
        done: false,
    };
    let mut conn = Conn {
        stdin,
        work_dir,
        counter,
    };
    while let Ok(Some(rawline)) = lines.next_line().await {
        if drive_line(&mut conn, &mut state, &rawline).await.is_err() || state.done {
            break;
        }
    }
    return DriveOutcome {
        done: state.done,
        msg: state.msg,
    };
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

/// Read the required `BRIDGE_PORT` env value.
///
/// # Errors
/// Returns `Err(())` after a stderr note when `BRIDGE_PORT` is unset.
fn read_port() -> Result<String, ()> {
    let Ok(port) = var("BRIDGE_PORT") else {
        discard(writeln!(stderr(), "BRIDGE_PORT env required (no fallback)"));
        return Err(());
    };
    return Ok(port);
}

/// Spawn codex, take its piped stdin/stdout, and send the `initialize` request.
///
/// # Errors
/// Returns `Err(())` when the spawn fails, a stdio handle is missing, or the initialize write fails.
async fn start_session(
    port: &str,
    counter: &mut u64,
) -> Result<(Child, ChildStdin, Lines<BufReader<ChildStdout>>), ()> {
    let Ok(mut child) = spawn_codex(port) else {
        return Err(());
    };
    let Some(mut stdin) = child.stdin.take() else {
        return Err(());
    };
    let Some(child_stdout) = child.stdout.take() else {
        return Err(());
    };
    let lines = BufReader::new(child_stdout).lines();
    let init = rpc_line(
        "initialize",
        &json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}),
        counter,
    );
    let Ok(()) = stdin.write_all(init.as_bytes()).await else {
        return Err(());
    };
    return Ok((child, stdin, lines));
}

/// Print the driver result line and the proof verdict line.
fn print_outcome(outcome: &DriveOutcome) {
    let done = outcome.done;
    let msg = &outcome.msg;
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
}

/// Run the spike: spawn codex app-server, drive it, print the proof line.
///
/// # Errors
/// Returns `Err(())` when work-dir creation, the required env read, the spawn, or the initialize write fails.
async fn run() -> Result<(), ()> {
    let work_dir = temp_dir().join(format!("rsdrv-{}", process_id()));
    let Ok(()) = create_dir_all(&work_dir) else {
        return Err(());
    };
    init_repo(&work_dir).await;
    let Ok(port) = read_port() else {
        return Err(());
    };
    let mut counter = 1_u64;
    let Ok((mut child, mut stdin, mut lines)) = start_session(&port, &mut counter).await else {
        return Err(());
    };
    let outcome = drive(&mut stdin, &mut lines, &work_dir, &mut counter).await;
    discard(child.kill().await);
    print_outcome(&outcome);
    return Ok(());
}

/// Spawn `codex app-server` wired to the BYOK provider on the bridge port.
///
/// # Errors
/// Returns the spawn `IoError` when the `codex` process fails to launch.
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
///
/// # Panics
/// Panics if the tokio runtime the `#[tokio::main]` macro builds fails to initialize.
#[tokio::main]
async fn main() -> ExitCode {
    return match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => ExitCode::FAILURE,
    };
}
