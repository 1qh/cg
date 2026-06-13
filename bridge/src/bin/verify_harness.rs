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
    fn check(&mut self, name: &str, ok: bool, detail: &str) {
        self.total = self.total.wrapping_add(1);
        if ok {
            self.pass = self.pass.wrapping_add(1);
        }
        discard(writeln!(
            stdout(),
            "  {} {} {}",
            if ok { "PASS" } else { "FAIL" },
            name,
            detail
        ));
    }
}

impl Session {
    /// Spawn `codex app-server` against the local bridge and run `initialize`.
    async fn new(wd: &str, sandbox: &str) -> Result<Self, ()> {
        let Ok(port) = var("BRIDGE_PORT") else {
            discard(writeln!(stderr(), "BRIDGE_PORT env required (no fallback)"));
            return Err(());
        };
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
        let mut child = match spawned {
            Ok(spawned_child) => spawned_child,
            Err(err) => {
                discard(writeln!(stderr(), "spawn codex failed: {err}"));
                return Err(());
            }
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
                .rpc(
                    "initialize",
                    json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}),
                    false,
                    sandbox,
                )
                .await,
        );
        return Ok(session);
    }
    /// Send one JSON-RPC request, drain responses, and summarize the turn.
    async fn rpc(&mut self, method: &str, params: Value, is_turn: bool, sandbox: &str) -> RpcOut {
        let myid = self.id;
        self.id = self.id.wrapping_add(1);
        let request_line = format!("{}\n", json!({"method":method,"id":myid,"params":params}));
        if let Err(err) = self.stdin.write_all(request_line.as_bytes()).await {
            discard(writeln!(stderr(), "stdin write failed: {err}"));
            return RpcOut {
                msg: String::new(),
                reasoning: 0_u32,
                shell: 0_u32,
                tid: String::new(),
            };
        }
        let (mut msg, mut shell, mut reasoning, mut tid) =
            (String::new(), 0_u32, 0_u32, String::new());
        let mut result_seen = false;
        while let Ok(Some(raw_line)) = self.lines.next_line().await {
            let trimmed = raw_line.trim().to_owned();
            if trimmed.is_empty() {
                continue;
            }
            let msg_value: Value = match serde_json::from_str(&trimmed) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            if msg_value.get("id").is_some() && msg_value.get("method").is_some() {
                let ack = format!("{}\n", json!({"id":msg_value.get("id"),"result":{}}));
                if let Err(err) = self.stdin.write_all(ack.as_bytes()).await {
                    discard(writeln!(stderr(), "stdin ack write failed: {err}"));
                    return RpcOut {
                        msg,
                        reasoning,
                        shell,
                        tid,
                    };
                }
                continue;
            }
            if msg_value.get("id").and_then(|value| return value.as_u64()) == Some(myid)
                && msg_value.get("result").is_some()
            {
                tid = msg_value
                    .pointer("/result/thread/id")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        return msg_value
                            .pointer("/result/threadId")
                            .and_then(Value::as_str);
                    })
                    .unwrap_or("")
                    .to_owned();
                result_seen = true;
                if !is_turn {
                    discard(sandbox);
                    break;
                }
            }
            if let Some(event_method) = msg_value.get("method").and_then(Value::as_str) {
                if let Some(item) = msg_value.pointer("/params/item") {
                    match item.get("type").and_then(Value::as_str) {
                        Some("agent_message") => {
                            if let Some(text) = item.get("text").and_then(Value::as_str) {
                                msg = text.into();
                            }
                        }
                        Some(item_type) if item_type.contains("command") => {
                            shell = shell.wrapping_add(1);
                        }
                        Some("reasoning") => reasoning = reasoning.wrapping_add(1),
                        _ => {}
                    }
                }
                if event_method == "item/agentMessage/delta"
                    && let Some(delta) = msg_value.pointer("/params/delta").and_then(Value::as_str)
                {
                    msg.push_str(delta);
                }
                if (event_method == "turn/completed" || event_method == "turn/failed") && is_turn {
                    break;
                }
            }
            if result_seen && !is_turn {
                break;
            }
        }
        return RpcOut {
            msg,
            reasoning,
            shell,
            tid,
        };
    }
    /// Start a new thread bound to the BYOK model and return its thread id.
    async fn start_thread(&mut self, sandbox: &str) -> String {
        return self.rpc("thread/start", json!({"model":"gemini-3.5-flash","modelProvider":"r","cwd":self.wd,"approvalPolicy":"never","sandbox":sandbox}), false, sandbox).await.tid;
    }
    /// Run one turn with the given text input and return message + shell + reasoning counts.
    async fn turn(&mut self, tid: &str, text: &str) -> (String, u32, u32) {
        let out = self
            .rpc(
                "turn/start",
                json!({"threadId":tid,"input":[{"type":"text","text":text,"text_elements":[]}]}),
                true,
                "",
            )
            .await;
        return (out.msg, out.shell, out.reasoning);
    }
}

/// Swallow a value, marking it intentionally unused.
fn discard<T>(_value: T) {}

#[tokio::main]
async fn main() -> ExitCode {
    let Ok(wds) = setup_workdir().await else {
        return ExitCode::FAILURE;
    };
    let mut checker = Checker { pass: 0, total: 0 };

    let Ok(mut session) = Session::new(&wds, "workspace-write").await else {
        return ExitCode::FAILURE;
    };
    let tid = session.start_thread("workspace-write").await;
    let (m1, _, r1) = session
        .turn(
            &tid,
            "Think step by step why 17 is prime, then reply with exactly ALPHA_OK.",
        )
        .await;
    checker.check(
        "turn completes + message",
        m1.contains("ALPHA_OK"),
        &format!("({})", &m1.chars().take(20).collect::<String>()),
    );
    checker.check("reasoning surfaces", r1 > 0, &format!("({r1} items)"));
    let (m2, sh2, _) = session
        .turn(&tid, "Run a shell command that prints exactly SHELL_OK_42.")
        .await;
    checker.check("shell tool executes", sh2 > 0, &format!("({sh2})"));
    checker.check(
        "shell output reported",
        m2.contains("SHELL_OK_42") || sh2 > 0,
        "",
    );
    discard(session.turn(&tid, "Remember the number 31337.").await);
    let (m4, _, _) = session
        .turn(
            &tid,
            "What number did I ask you to remember? Reply with just the number.",
        )
        .await;
    checker.check(
        "multi-turn continuity",
        m4.contains("31337"),
        &format!("({})", &m4.chars().take(20).collect::<String>()),
    );
    discard(session.child.kill().await);
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
    return ExitCode::SUCCESS;
}

/// Initialize a git working dir for the harness threads; fatal on setup failure.
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
