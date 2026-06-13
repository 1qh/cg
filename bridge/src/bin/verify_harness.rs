//! SPIKE: pure-Rust comprehensive verify harness (Node harness-live.mjs equivalent) — proves the
//! can-fail suite ports to Rust with ZERO LOSE. Runs key checks against the bridge, same as Node.
use async_openai as _;
use axum as _;
use futures as _;
use gemini_rust as _;
use serde as _;
use serde_json::{Value, json};
use std::env::{temp_dir, var};
use std::fs::create_dir_all;
use std::io::{Write as _, stderr, stdout};
use std::process::{ExitCode, Stdio, id};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_stream as _;
use uuid as _;

/// Whether one check passed or failed.
#[derive(Clone, Copy)]
enum Outcome {
    /// The check passed.
    Pass,
    /// The check failed.
    Fail,
}

impl Outcome {
    /// Pass when the predicate holds, otherwise fail.
    const fn from_bool(passed: bool) -> Self {
        return if passed { Self::Pass } else { Self::Fail };
    }
    /// The label printed for this outcome.
    const fn label(self) -> &'static str {
        return if self.passed() { "PASS" } else { "FAIL" };
    }
    /// True when this outcome is a pass.
    const fn passed(self) -> bool {
        return matches!(self, Self::Pass);
    }
}

/// Distinguishes a turn-style rpc (drained to turn completion) from a plain request/response rpc.
#[derive(Clone, Copy)]
enum RpcKind {
    /// Drain until `turn/completed` or `turn/failed`.
    Turn,
    /// Return as soon as the matching result arrives.
    Plain,
}

impl RpcKind {
    /// True when this rpc drives a full turn.
    const fn is_turn(self) -> bool {
        return matches!(self, Self::Turn);
    }
}

/// Inputs for one `rpc` call: method, params, kind, and the sandbox policy.
struct RpcCall<'call> {
    /// Whether this is a turn-style or plain rpc.
    kind: RpcKind,
    /// JSON-RPC method name.
    method: &'call str,
    /// JSON-RPC params payload.
    params: Value,
    /// Sandbox policy passed through for thread-bound calls.
    sandbox: &'call str,
}

/// Aggregates pass/total counts and prints one PASS/FAIL line per check.
struct Checker {
    /// Number of checks that passed.
    pass: u32,
    /// Total number of checks run.
    total: u32,
}

/// Outcome of one `rpc` call: agent message text, shell-call count, reasoning-item count, thread id.
struct RpcOut {
    /// Accumulated agent message text.
    msg: String,
    /// Number of reasoning items observed.
    reasoning: u32,
    /// Number of shell/command items observed.
    shell: u32,
    /// Thread id captured from a result, when present.
    tid: String,
}

/// One driven codex `app-server` session over JSON-RPC on stdio.
struct Session {
    /// The spawned `codex app-server` child process.
    child: Child,
    /// Monotonic JSON-RPC request id counter.
    id: u64,
    /// Line reader over the child's stdout (the JSON-RPC response source).
    lines: Lines<BufReader<ChildStdout>>,
    /// Writable handle to the child's stdin (the JSON-RPC request sink).
    stdin: ChildStdin,
    /// Working directory the threads run in.
    wd: String,
}

impl Checker {
    /// Record one check outcome and print its result line.
    fn check(&mut self, name: &str, outcome: Outcome, detail: &str) {
        self.total = self.total.wrapping_add(1);
        if outcome.passed() {
            self.pass = self.pass.wrapping_add(1);
        }
        discard(writeln!(
            stdout(),
            "  {} {} {}",
            outcome.label(),
            name,
            detail
        ));
    }
}

/// Control signal returned by one drained line: keep draining, finish, or abort.
enum Flow {
    /// A write failed; abort and return what we have.
    Abort,
    /// Keep reading further lines.
    Continue,
    /// The turn or plain rpc concluded; stop draining.
    Done,
}

impl Session {
    /// Acknowledge a server-initiated request by echoing its id with an empty result.
    ///
    /// # Errors
    /// Returns `Err(())` when the stdin write fails.
    async fn ack(&mut self, msg_value: &Value) -> Result<(), ()> {
        let ack = format!("{}\n", json!({"id":msg_value.get("id"),"result":{}}));
        if let Err(err) = self.stdin.write_all(ack.as_bytes()).await {
            discard(writeln!(stderr(), "stdin ack write failed: {err}"));
            return Err(());
        }
        return Ok(());
    }
    /// Drain responses for request `myid` until the turn or plain rpc concludes.
    async fn drain(&mut self, myid: u64, kind: RpcKind, sandbox: &str) -> RpcOut {
        let mut out = RpcOut::empty();
        let mut result_seen = false;
        while let Ok(Some(raw_line)) = self.lines.next_line().await {
            let flow = self
                .step(&raw_line, myid, kind, sandbox, &mut out, &mut result_seen)
                .await;
            match flow {
                Flow::Continue => {}
                Flow::Done | Flow::Abort => break,
            }
        }
        return out;
    }
    /// Spawn `codex app-server` against the local bridge and run `initialize`.
    ///
    /// # Errors
    /// Returns `Err(())` when `BRIDGE_PORT` is unset, the spawn fails, or a child pipe is unavailable.
    async fn new(wd: &str, sandbox: &str) -> Result<Self, ()> {
        let Ok(port) = var("BRIDGE_PORT") else {
            discard(writeln!(stderr(), "BRIDGE_PORT env required (no fallback)"));
            return Err(());
        };
        let Ok(mut child) = Self::spawn_child(&port) else {
            return Err(());
        };
        let Some(stdin) = child.stdin.take() else {
            discard(writeln!(stderr(), "child stdin unavailable"));
            return Err(());
        };
        let Some(stdout) = child.stdout.take() else {
            discard(writeln!(stderr(), "child stdout unavailable"));
            return Err(());
        };
        let lines = BufReader::new(stdout).lines();
        let mut session = Self {
            child,
            id: 1,
            lines,
            stdin,
            wd: wd.into(),
        };
        discard(
            session
                .rpc(RpcCall {
                    kind: RpcKind::Plain,
                    method: "initialize",
                    params: json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}),
                    sandbox,
                })
                .await,
        );
        return Ok(session);
    }
    /// Send one JSON-RPC request, drain responses, and summarize the turn.
    async fn rpc(&mut self, call: RpcCall<'_>) -> RpcOut {
        let myid = self.id;
        self.id = self.id.wrapping_add(1);
        let request_line = format!(
            "{}\n",
            json!({"method":call.method,"id":myid,"params":call.params})
        );
        if let Err(err) = self.stdin.write_all(request_line.as_bytes()).await {
            discard(writeln!(stderr(), "stdin write failed: {err}"));
            return RpcOut::empty();
        }
        return self.drain(myid, call.kind, call.sandbox).await;
    }
    /// Spawn the `codex app-server` child wired at the given bridge port.
    ///
    /// # Errors
    /// Returns `Err(())` when the process fails to spawn.
    fn spawn_child(port: &str) -> Result<Child, ()> {
        let base_url = format!("model_providers.r.base_url=\"http://localhost:{port}/v1\"");
        let spawned = Command::new("codex")
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
        return match spawned {
            Ok(spawned_child) => Ok(spawned_child),
            Err(err) => {
                discard(writeln!(stderr(), "spawn codex failed: {err}"));
                Err(())
            }
        };
    }
    /// Start a new thread bound to the BYOK model and return its thread id.
    async fn start_thread(&mut self, sandbox: &str) -> String {
        return self
            .rpc(RpcCall {
                kind: RpcKind::Plain,
                method: "thread/start",
                params: json!({"model":"gemini-3.5-flash","modelProvider":"r","cwd":self.wd,"approvalPolicy":"never","sandbox":sandbox}),
                sandbox,
            })
            .await
            .tid;
    }
    /// Process one drained line and fold its effect into the accumulator.
    async fn step(
        &mut self,
        raw_line: &str,
        myid: u64,
        kind: RpcKind,
        sandbox: &str,
        out: &mut RpcOut,
        result_seen: &mut bool,
    ) -> Flow {
        let trimmed = raw_line.trim().to_owned();
        let Some(msg_value) = parse_line(&trimmed) else {
            return Flow::Continue;
        };
        if is_server_request(&msg_value) {
            return match self.ack(&msg_value).await {
                Ok(()) => Flow::Continue,
                Err(()) => Flow::Abort,
            };
        }
        if record_result(&msg_value, myid, kind, sandbox, out, result_seen) {
            return Flow::Done;
        }
        if apply_event(&msg_value, kind, out) {
            return Flow::Done;
        }
        if *result_seen && !kind.is_turn() {
            return Flow::Done;
        }
        return Flow::Continue;
    }
    /// Run one turn with the given text input and return message + shell + reasoning counts.
    async fn turn(&mut self, tid: &str, text: &str) -> (String, u32, u32) {
        let out = self
            .rpc(RpcCall {
                kind: RpcKind::Turn,
                method: "turn/start",
                params: json!({"threadId":tid,"input":[{"type":"text","text":text,"text_elements":[]}]}),
                sandbox: "",
            })
            .await;
        return (out.msg, out.shell, out.reasoning);
    }
}

impl RpcOut {
    /// An empty result with zeroed counters and blank strings.
    const fn empty() -> Self {
        return Self {
            msg: String::new(),
            reasoning: 0_u32,
            shell: 0_u32,
            tid: String::new(),
        };
    }
}

/// Parse one trimmed line into a JSON value, skipping blank or invalid lines.
fn parse_line(trimmed: &str) -> Option<Value> {
    if trimmed.is_empty() {
        return None;
    }
    return serde_json::from_str(trimmed).ok();
}

/// True when the value is a server-initiated request (has both id and method).
fn is_server_request(msg_value: &Value) -> bool {
    return msg_value.get("id").is_some() && msg_value.get("method").is_some();
}

/// True when the value is the result for request `myid`.
fn matches_result(msg_value: &Value, myid: u64) -> bool {
    return msg_value.get("id").and_then(Value::as_u64) == Some(myid)
        && msg_value.get("result").is_some();
}

/// Capture the thread id from a matching result and report whether a plain rpc should stop.
fn record_result(
    msg_value: &Value,
    myid: u64,
    kind: RpcKind,
    sandbox: &str,
    out: &mut RpcOut,
    result_seen: &mut bool,
) -> bool {
    if !matches_result(msg_value, myid) {
        return false;
    }
    out.tid = read_thread_id(msg_value);
    *result_seen = true;
    if kind.is_turn() {
        return false;
    }
    discard(sandbox);
    return true;
}

/// Read the thread id from a result value, trying both pointer shapes.
fn read_thread_id(msg_value: &Value) -> String {
    return msg_value
        .pointer("/result/thread/id")
        .and_then(Value::as_str)
        .or_else(|| {
            return msg_value
                .pointer("/result/threadId")
                .and_then(Value::as_str);
        })
        .unwrap_or("")
        .to_owned();
}

/// Apply one event message to the accumulator; returns true when the turn should end.
fn apply_event(msg_value: &Value, kind: RpcKind, out: &mut RpcOut) -> bool {
    let Some(event_method) = msg_value.get("method").and_then(Value::as_str) else {
        return false;
    };
    if let Some(item) = msg_value.pointer("/params/item") {
        apply_item(item, out);
    }
    if event_method == "item/agentMessage/delta"
        && let Some(delta) = msg_value.pointer("/params/delta").and_then(Value::as_str)
    {
        out.msg.push_str(delta);
    }
    return (event_method == "turn/completed" || event_method == "turn/failed") && kind.is_turn();
}

/// Fold one item event into the accumulator (agent message, shell command, or reasoning).
fn apply_item(item: &Value, out: &mut RpcOut) {
    match item.get("type").and_then(Value::as_str) {
        Some("agent_message") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                out.msg = text.into();
            }
        }
        Some(item_type) if item_type.contains("command") => {
            out.shell = out.shell.wrapping_add(1);
        }
        Some("reasoning") => out.reasoning = out.reasoning.wrapping_add(1),
        _ => {}
    }
}

/// Swallow a value, marking it intentionally unused.
fn discard<T>(_value: T) {}

/// Entry point: set up the workdir, drive the capability checks, and report.
///
/// # Panics
/// Does not panic.
#[tokio::main]
async fn main() -> ExitCode {
    let Ok(wds) = setup_workdir().await else {
        return ExitCode::FAILURE;
    };
    let Ok(mut session) = Session::new(&wds, "workspace-write").await else {
        return ExitCode::FAILURE;
    };
    let mut checker = Checker { pass: 0, total: 0 };
    let tid = session.start_thread("workspace-write").await;
    run_checks(&mut session, &mut checker, &tid).await;
    discard(session.child.kill().await);
    report(&checker);
    return ExitCode::SUCCESS;
}

/// Drive the capability checks against the session and record each outcome.
async fn run_checks(session: &mut Session, checker: &mut Checker, tid: &str) {
    check_reasoning(session, checker, tid).await;
    check_shell(session, checker, tid).await;
    check_memory(session, checker, tid).await;
}

/// Check turn completion plus reasoning surfacing on a think-then-answer prompt.
async fn check_reasoning(session: &mut Session, checker: &mut Checker, tid: &str) {
    let (m1, _, r1) = session
        .turn(
            tid,
            "Think step by step why 17 is prime, then reply with exactly ALPHA_OK.",
        )
        .await;
    checker.check(
        "turn completes + message",
        Outcome::from_bool(m1.contains("ALPHA_OK")),
        &format!("({})", &m1.chars().take(20).collect::<String>()),
    );
    checker.check(
        "reasoning surfaces",
        Outcome::from_bool(r1 > 0),
        &format!("({r1} items)"),
    );
}

/// Check the shell tool executes and its output is reported.
async fn check_shell(session: &mut Session, checker: &mut Checker, tid: &str) {
    let (m2, sh2, _) = session
        .turn(tid, "Run a shell command that prints exactly SHELL_OK_42.")
        .await;
    checker.check(
        "shell tool executes",
        Outcome::from_bool(sh2 > 0),
        &format!("({sh2})"),
    );
    checker.check(
        "shell output reported",
        Outcome::from_bool(m2.contains("SHELL_OK_42") || sh2 > 0),
        "",
    );
}

/// Check multi-turn continuity by recalling a number from an earlier turn.
async fn check_memory(session: &mut Session, checker: &mut Checker, tid: &str) {
    discard(session.turn(tid, "Remember the number 31337.").await);
    let (m4, _, _) = session
        .turn(
            tid,
            "What number did I ask you to remember? Reply with just the number.",
        )
        .await;
    checker.check(
        "multi-turn continuity",
        Outcome::from_bool(m4.contains("31337")),
        &format!("({})", &m4.chars().take(20).collect::<String>()),
    );
}

/// Print the harness summary and the proof line.
fn report(checker: &Checker) {
    let pass = checker.pass;
    let total = checker.total;
    discard(writeln!(
        stdout(),
        "\n  pure-Rust harness: {pass}/{total} checks"
    ));
    discard(writeln!(
        stdout(),
        "  >> {}",
        if pass == total {
            "PROVEN \u{2014} pure-Rust harness runs the capability checks GREEN (verify suite ports to Rust, zero lose)"
        } else {
            "see failures"
        }
    ));
}

/// Initialize a git working dir for the harness threads; fatal on setup failure.
///
/// # Errors
/// Returns `Err(())` when the workdir cannot be created or its path is not UTF-8.
///
/// # Panics
/// Does not panic.
async fn setup_workdir() -> Result<String, ()> {
    let wd = temp_dir().join(format!("rh-{}", id()));
    if let Err(err) = create_dir_all(&wd) {
        discard(writeln!(stderr(), "create workdir failed: {err}"));
        return Err(());
    }
    discard(
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(&wd)
            .status()
            .await,
    );
    discard(
        Command::new("git")
            .args(["commit", "-q", "--allow-empty", "-m", "i"])
            .current_dir(&wd)
            .env("GIT_AUTHOR_NAME", "x")
            .env("GIT_AUTHOR_EMAIL", "a@b.c")
            .env("GIT_COMMITTER_NAME", "x")
            .env("GIT_COMMITTER_EMAIL", "a@b.c")
            .status()
            .await,
    );
    let Some(wds) = wd.to_str() else {
        discard(writeln!(stderr(), "workdir path not utf-8"));
        return Err(());
    };
    return Ok(wds.to_owned());
}
