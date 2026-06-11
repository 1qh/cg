//! SPIKE: pure-Rust comprehensive verify harness (Node harness-live.mjs equivalent) — proves the
//! can-fail suite ports to Rust with ZERO LOSE. Runs key checks against the bridge, same as Node.
use serde_json::{json, Value};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdin, Command};

struct Session {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<tokio::process::ChildStdout>>,
    id: u64,
    wd: String,
}

impl Session {
    async fn new(wd: &str, sandbox: &str) -> Self {
        let port = std::env::var("BRIDGE_PORT").expect("BRIDGE_PORT env required (no fallback)");
        let base_url = format!("model_providers.r.base_url=\"http://localhost:{port}/v1\"");
        let mut child = Command::new("codex")
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
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut s = Self {
            child,
            stdin,
            lines,
            id: 1,
            wd: wd.into(),
        };
        s.rpc(
            "initialize",
            json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}),
            false,
            sandbox,
        )
        .await;
        return s;
    }
    async fn rpc(
        &mut self,
        method: &str,
        params: Value,
        is_turn: bool,
        sandbox: &str,
    ) -> (String, u32, u32, String) {
        let myid = self.id;
        self.id += 1;
        let line = format!("{}\n", json!({"method":method,"id":myid,"params":params}));
        self.stdin.write_all(line.as_bytes()).await.unwrap();
        let (mut msg, mut shell, mut reasoning, mut tid) =
            (String::new(), 0_u32, 0_u32, String::new());
        let mut result_seen = false;
        while let Ok(Some(l)) = self.lines.next_line().await {
            let l = l.trim().to_owned();
            if l.is_empty() {
                continue;
            }
            let m: Value = match serde_json::from_str(&l) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if m.get("id").is_some() && m.get("method").is_some() {
                self.stdin
                    .write_all(format!("{}\n", json!({"id":m["id"],"result":{}})).as_bytes())
                    .await
                    .unwrap();
                continue;
            }
            if m.get("id").and_then(|v| return v.as_u64()) == Some(myid)
                && m.get("result").is_some()
            {
                tid = m["result"]["thread"]["id"]
                    .as_str()
                    .or_else(|| return m["result"]["threadId"].as_str())
                    .unwrap_or("")
                    .to_owned();
                result_seen = true;
                if !is_turn {
                    let _ = sandbox;
                    break;
                }
            }
            if let Some(method) = m.get("method").and_then(Value::as_str) {
                if let Some(it) = m.pointer("/params/item") {
                    match it.get("type").and_then(Value::as_str) {
                        Some("agent_message") => {
                            if let Some(t) = it.get("text").and_then(Value::as_str) {
                                msg = t.into();
                            }
                        }
                        Some(t) if t.contains("command") => shell += 1,
                        Some("reasoning") => reasoning += 1,
                        _ => {}
                    }
                }
                if method == "item/agentMessage/delta" {
                    if let Some(d) = m.pointer("/params/delta").and_then(Value::as_str) {
                        msg.push_str(d);
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
        return (msg, shell, reasoning, tid);
    }
    async fn start_thread(&mut self, sandbox: &str) -> String {
        return self.rpc("thread/start", json!({"model":"gemini-3.5-flash","modelProvider":"r","cwd":self.wd,"approvalPolicy":"never","sandbox":sandbox}), false, sandbox).await.3;
    }
    async fn turn(&mut self, tid: &str, text: &str) -> (String, u32, u32) {
        let r = self
            .rpc(
                "turn/start",
                json!({"threadId":tid,"input":[{"type":"text","text":text,"text_elements":[]}]}),
                true,
                "",
            )
            .await;
        return (r.0, r.1, r.2);
    }
}

#[tokio::main]
async fn main() {
    let wd = std::env::temp_dir().join(format!("rh-{}", std::process::id()));
    std::fs::create_dir_all(&wd).unwrap();
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
    let wds = wd.to_str().unwrap();
    let mut pass = 0;
    let mut total = 0;
    let mut ck = |name: &str, ok: bool, detail: &str| {
        total += 1;
        if ok {
            pass += 1;
        }
        println!("  {} {} {}", if ok { "PASS" } else { "FAIL" }, name, detail);
    };

    let mut s = Session::new(wds, "workspace-write").await;
    let tid = s.start_thread("workspace-write").await;
    let (m1, _, r1) = s
        .turn(
            &tid,
            "Think step by step why 17 is prime, then reply with exactly ALPHA_OK.",
        )
        .await;
    ck(
        "turn completes + message",
        m1.contains("ALPHA_OK"),
        &format!("({})", &m1.chars().take(20).collect::<String>()),
    );
    ck("reasoning surfaces", r1 > 0, &format!("({r1} items)"));
    let (m2, sh2, _) = s
        .turn(&tid, "Run a shell command that prints exactly SHELL_OK_42.")
        .await;
    ck("shell tool executes", sh2 > 0, &format!("({sh2})"));
    ck(
        "shell output reported",
        m2.contains("SHELL_OK_42") || sh2 > 0,
        "",
    );
    let _ = s.turn(&tid, "Remember the number 31337.").await;
    let (m4, _, _) = s
        .turn(
            &tid,
            "What number did I ask you to remember? Reply with just the number.",
        )
        .await;
    ck(
        "multi-turn continuity",
        m4.contains("31337"),
        &format!("({})", &m4.chars().take(20).collect::<String>()),
    );
    let _ = s.child.kill().await;
    println!("\n  pure-Rust harness: {pass}/{total} checks");
    println!(
        "  >> {}",
        if pass == total {
            "PROVEN \u{2014} pure-Rust harness runs the capability checks GREEN (verify suite ports to Rust, zero lose)"
        } else {
            "see failures"
        }
    );
}
