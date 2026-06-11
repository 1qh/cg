//! SPIKE: can PURE RUST drive codex app-server over JSON-RPC (the verify-harness side)?
//! Spawn codex app-server -> initialize -> thread/start -> turn/start -> capture agent message + completion.
//! If this works, the whole product (bridge + launcher + can-fail suite) can be pure Rust.
use serde_json::{json, Value};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[tokio::main]
async fn main() {
    let wd = std::env::temp_dir().join(format!("rsdrv-{}", std::process::id()));
    std::fs::create_dir_all(&wd).unwrap();
    let _ = Command::new("git").args(["init", "-q"]).current_dir(&wd).status().await;
    let _ = Command::new("git").args(["commit", "-q", "--allow-empty", "-m", "i"])
        .current_dir(&wd).env("GIT_AUTHOR_NAME", "x").env("GIT_AUTHOR_EMAIL", "a@b.c")
        .env("GIT_COMMITTER_NAME", "x").env("GIT_COMMITTER_EMAIL", "a@b.c").status().await;

    let port = std::env::var("BRIDGE_PORT").expect("BRIDGE_PORT env required (no fallback)");
    let base_url = format!("model_providers.r.base_url=\"http://localhost:{port}/v1\"");
    let mut child = Command::new("codex")
        .args(["app-server",
            "-c", "model_provider=r", "-c", "model_providers.r.name=\"r\"",
            "-c", &base_url,
            "-c", "model_providers.r.wire_api=\"responses\"", "-c", "model_providers.r.env_key=\"K\""])
        .env("K", "x").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().expect("spawn codex");
    let mut stdin = child.stdin.take().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut id = 1u64;
    let mut send = |m: &str, p: Value, id: &mut u64| {
        let line = format!("{}\n", json!({"method": m, "id": *id, "params": p}));
        *id += 1; line
    };
    stdin.write_all(send("initialize", json!({"clientInfo":{"name":"x","version":"0"},"capabilities":null}), &mut id).as_bytes()).await.unwrap();

    let mut thread_id = String::new();
    let mut msg = String::new();
    let mut done = false;
    let mut started = false;
    while let Ok(Some(l)) = lines.next_line().await {
        let l = l.trim().to_string();
        if l.is_empty() { continue; }
        let m: Value = match serde_json::from_str(&l) { Ok(v) => v, Err(_) => continue };
        // server->client request: ack with empty result
        if m.get("id").is_some() && m.get("method").is_some() {
            stdin.write_all(format!("{}\n", json!({"id": m["id"], "result": {}})).as_bytes()).await.unwrap();
            continue;
        }
        // initialize result -> start thread
        if m.get("id").is_some() && m.get("result").is_some() && !started {
            if thread_id.is_empty() {
                stdin.write_all(send("thread/start", json!({"model":"gemini-3.5-flash","modelProvider":"r","cwd":wd.to_str().unwrap(),"approvalPolicy":"never","sandbox":"read-only"}), &mut id).as_bytes()).await.unwrap();
                thread_id = "pending".into();
            } else if thread_id == "pending" {
                let tid = m["result"]["thread"]["id"].as_str().or_else(|| m["result"]["threadId"].as_str()).unwrap_or("").to_string();
                thread_id = tid.clone();
                stdin.write_all(send("turn/start", json!({"threadId":tid,"input":[{"type":"text","text":"Reply with exactly: PURE_RUST_DRIVER_OK","text_elements":[]}]}), &mut id).as_bytes()).await.unwrap();
                started = true;
            }
        }
        // notifications
        if let Some(method) = m.get("method").and_then(Value::as_str) {
            if let Some(it) = m.pointer("/params/item") {
                if it.get("type").and_then(Value::as_str) == Some("agent_message") {
                    if let Some(t) = it.get("text").and_then(Value::as_str) { msg = t.to_string(); }
                }
            }
            if method == "item/agentMessage/delta" {
                if let Some(d) = m.pointer("/params/delta").and_then(Value::as_str) { msg.push_str(d); }
            }
            if method == "turn/completed" || method == "turn/failed" { done = true; }
        }
        if done { break; }
    }
    let _ = child.kill().await;
    println!("  pure-Rust driver: done={done} reply={msg:?}");
    println!("  >> {}", if done && msg.contains("PURE_RUST_DRIVER_OK") { "PROVEN — pure Rust drives codex app-server e2e (verify harnesses can be Rust)" } else { "see above" });
}
