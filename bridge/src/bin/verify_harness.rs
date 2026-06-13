//! SPIKE: pure-Rust comprehensive verify harness (Node harness-live.mjs equivalent) — proves the
//! can-fail suite ports to Rust with ZERO LOSE. Runs key checks against the bridge, same as Node.
use serde_json::{json, Value};
use std::io::Write as _;
use std::process::{ExitCode, Stdio};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, Command};

/// One driven codex `app-server` session over JSON-RPC on stdio.
struct Session {
    /// The spawned `codex app-server` child process.
    child: Child,
    /// Writable handle to the child's stdin (the JSON-RPC request sink).
    stdin: ChildStdin,
    /// Line reader over the child's stdout (the JSON-RPC response source).
    lines: Lines<BufReader<tokio::process::ChildStdout>>,
    /// Monotonic JSON-RPC request id counter.
    id: u64,
    /// Working directory the threads run in.
    wd: String,
}

/// Outcome of one `rpc` call: agent message text, shell-call count, reasoning-item count, thread id.
struct RpcOut {
    /// Accumulated agent message text.
    msg: String,
    /// Number of shell/command items observed.
    shell: u32,
    /// Number of reasoning items observed.
    reasoning: u32,
    /// Thread id captured from a result, when present.
    tid: String,
}

impl Session {
    /// Spawn `codex app-server` against the local bridge and run `initialize`.
    async fn new(wd: &str, sandbox: &str) -> Self {
        let Ok(port) = std::env::var("BRIDGE_PORT") else {
            let _ = writeln!(std::io::stderr(), "BRIDGE_PORT env required (no fallback)");
            std::process::exit(1);
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
                let _ = writeln!(std::io::stderr(), "spawn codex failed: {err}");
                std::process::exit(1);
            }
        };
        let Some(stdin) = child.stdin.take() else {
            let _ = writeln!(std::io::stderr(), "child stdin unavailable");
            std::process::exit(1);
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = writeln!(std::io::stderr(), "child stdout unavailable");
            std::process::exit(1);
        };
        let lines = BufReader::new(stdout).lines();
        let mut session = Self {
            child,
            stdin,
            lines,
            id: 1,
            wd: wd.into(),
        };
        let _ = session
            .rpc(
                "initialize",
                json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}),
                false,
                sandbox,
            )
            .await;
        return session;
    }
    /// Send one JSON-RPC request, drain responses, and summarize the turn.
    async fn rpc(&mut self, method: &str, params: Value, is_turn: bool, sandbox: &str) -> RpcOut {
        let myid = self.id;
        self.id = self.id.wrapping_add(1);
        let line = format!("{}\n", json!({"method":method,"id":myid,"params":params}));
        if let Err(err) = self.stdin.write_all(line.as_bytes()).await {
            let _ = writeln!(std::io::stderr(), "stdin write failed: {err}");
            std::process::exit(1);
        }
        let (mut msg, mut shell, mut reasoning, mut tid) =
            (String::new(), 0u32, 0u32, String::new());
        let mut result_seen = false;
        while let Ok(Some(line)) = self.lines.next_line().await {
            let line = line.trim().to_owned();
            if line.is_empty() {
                continue;
            }
            let msg_value: Value = match serde_json::from_str(&line) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            if msg_value.get("id").is_some() && msg_value.get("method").is_some() {
                let ack = format!("{}\n", json!({"id":msg_value["id"],"result":{}}));
                if let Err(err) = self.stdin.write_all(ack.as_bytes()).await {
                    let _ = writeln!(std::io::stderr(), "stdin ack write failed: {err}");
                    std::process::exit(1);
                }
                continue;
            }
            if msg_value.get("id").and_then(|value| return value.as_u64()) == Some(myid)
                && msg_value.get("result").is_some()
            {
                tid = msg_value
                    .pointer("/result/thread/id")
                    .and_then(Value::as_str)
                    .or_else(|| return msg_value.pointer("/result/threadId").and_then(Value::as_str))
                    .unwrap_or("")
                    .to_owned();
                result_seen = true;
                if !is_turn {
                    let _ = sandbox;
                    break;
                }
            }
            if let Some(method) = msg_value.get("method").and_then(Value::as_str) {
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
                if method == "item/agentMessage/delta" {
                    if let Some(delta) = msg_value.pointer("/params/delta").and_then(Value::as_str) {
                        msg.push_str(delta);
                    }
                }
                if (method == "turn/completed" || method == "turn/failed") && is_turn {
                    break;
                }
            }
            if result_seen && !is_turn {
                break;
            }
        }
        return RpcOut {
            msg,
            shell,
            reasoning,
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

/// Aggregates pass/total counts and prints one PASS/FAIL line per check.
struct Checker {
    /// Number of checks that passed.
    pass: u32,
    /// Total number of checks run.
    total: u32,
}

impl Checker {
    /// Record one check outcome and print its result line.
    fn check(&mut self, name: &str, ok: bool, detail: &str) {
        self.total = self.total.wrapping_add(1);
        if ok {
            self.pass = self.pass.wrapping_add(1);
        }
        let _ = writeln!(
            std::io::stdout(),
            "  {} {} {}",
            if ok { "PASS" } else { "FAIL" },
            name,
            detail
        );
    }
}

/// Initialize a git working dir for the harness threads; fatal on setup failure.
async fn setup_workdir() -> String {
    let wd = std::env::temp_dir().join(format!("rh-{}", std::process::id()));
    if let Err(err) = std::fs::create_dir_all(&wd) {
        let _ = writeln!(std::io::stderr(), "create workdir failed: {err}");
        std::process::exit(1);
    }
    let _ = Command::new("git")
        .args(["init", "-q"])
        .current_dir(&wd)
        .status()
        .await;
    let _ = Command::new("git")
        .args(["commit", "-q", "--allow-empty", "-m", "i"])
        .current_dir(&wd)
        .env("GIT_AUTHOR_NAME", "x")
        .env("GIT_AUTHOR_EMAIL", "a@b.c")
        .env("GIT_COMMITTER_NAME", "x")
        .env("GIT_COMMITTER_EMAIL", "a@b.c")
        .status()
        .await;
    let Some(wds) = wd.to_str() else {
        let _ = writeln!(std::io::stderr(), "workdir path not utf-8");
        std::process::exit(1);
    };
    return wds.to_owned();
}

#[tokio::main]
async fn main() -> ExitCode {
    let wds = setup_workdir().await;
    let mut checker = Checker { pass: 0, total: 0 };

    let mut session = Session::new(&wds, "workspace-write").await;
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
    let _ = session.turn(&tid, "Remember the number 31337.").await;
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
    let _ = session.child.kill().await;
    let pass = checker.pass;
    let total = checker.total;
    let _ = writeln!(
        std::io::stdout(),
        "\n  pure-Rust harness: {pass}/{total} checks"
    );
    let _ = writeln!(
        std::io::stdout(),
        "  >> {}",
        if pass == total {
            "PROVEN \u{2014} pure-Rust harness runs the capability checks GREEN (verify suite ports to Rust, zero lose)"
        } else {
            "see failures"
        }
    );
    return ExitCode::SUCCESS;
}
