// Can-fail probe of MCP tool calling through the harness on the CURRENT native stack. The spike found
// native-macOS MCP routing broken (works via container); this re-measures it. Wires a minimal MCP stdio
// server via mcp_servers config, asks for the secret number, checks for an mcp_tool_call item + the answer.
// Reports the real outcome (this is a spike: a documented gap is a valid result). Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4022;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");
const MCP = join(ROOT, "verify", "fixtures", "mcp-server.mjs");

function startBridge() {
  const fd = openSync("/tmp/mcp-live-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`MCP tool call through the harness (${MODEL})`, { timeout: 180000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  let srv;
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const WS = mkdtempSync(join(tmpdir(), "mcp-"));
    execSync("git init -q && git commit -q --allow-empty -m i", { cwd: WS, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
    const cfg = ["-c","model_provider=litellm","-c",'model_providers.litellm.name="g"',
      "-c",`model_providers.litellm.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.litellm.wire_api="responses"',
      "-c",'model_providers.litellm.env_key="LITELLM_KEY"',"-c",'model_reasoning_effort="high"',"-c",`model_catalog_json="${CAT}"`,
      "-c",'mcp_servers.verifytools.command="node"',"-c",`mcp_servers.verifytools.args=["${MCP}"]`];
    srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, LITELLM_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
    let id = 1; const pend = new Map(); let buf = "", done = false, failed = null, msg = "", sawMcp = false;
    const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
    srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
      if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
      if (m.id != null && m.method) { srv.stdin.write(JSON.stringify({ id: m.id, result: {} }) + "\n"); continue }
      const it = m.params?.item; const t = it?.type || "";
      if (/mcp/i.test(t) || /mcp/i.test(m.method || "")) sawMcp = true;
      if (t === "agent_message") msg = (it.text || msg);
      if (m.method === "item/agentMessage/delta") msg += (m.params?.delta || "");
      if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,100); done = true } } });

    await send("initialize", { clientInfo: { name: "mcp", version: "0" }, capabilities: null });
    const ts = await send("thread/start", { model: MODEL, modelProvider: "litellm", cwd: WS, approvalPolicy: "never", sandbox: "workspace-write" });
    const threadId = ts?.thread?.id ?? ts?.threadId;
    await send("turn/start", { threadId, input: [{ type: "text", text: "Call the get_secret_number tool and tell me the secret number.", text_elements: [] }] });
    const t0 = Date.now(); while (!done && Date.now() - t0 < 90000) await new Promise(r => setTimeout(r, 400));

    console.log(`  ${MODEL}: mcp_tool_call item=${sawMcp} | answer=${JSON.stringify(msg.slice(0,60))} | failed=${failed}`);
    assert.ok(!failed, `MCP turn failed: ${failed}`);
    assert.ok(sawMcp || /42/.test(msg), "MCP tool must be called (mcp_tool_call item) or its result (42) must reach the answer");
  } finally {
    srv?.kill("SIGKILL");
    bridge.kill("SIGKILL");
  }
});
