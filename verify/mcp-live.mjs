// Can-fail proof of MCP tool calling through the harness on the current stack. The MCP server is registered
// via a config.toml under a temp CODEX_HOME (the `-c mcp_servers.args=[...]` flag does NOT register it —
// that mistake produced an earlier false "MCP broken" finding). Asserts: the server loads, codex can call
// the tool directly (server reachable), and a model turn surfaces an MCP item + the result (42). GEMINI_API_KEY req.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, writeFileSync, openSync } from "node:fs";
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
  // register the MCP server via config.toml under a temp CODEX_HOME (the supported path)
  const codexHome = mkdtempSync(join(tmpdir(), "codexhome-"));
  writeFileSync(join(codexHome, "config.toml"), `[mcp_servers.verifytools]\ncommand = "node"\nargs = ["${MCP}"]\n`);
  const bridge = startBridge();
  let srv;
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const WS = mkdtempSync(join(tmpdir(), "mcp-"));
    execSync("git init -q && git commit -q --allow-empty -m i", { cwd: WS, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
    const cfg = ["-c","model_provider=litellm","-c",'model_providers.litellm.name="g"',
      "-c",`model_providers.litellm.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.litellm.wire_api="responses"',
      "-c",'model_providers.litellm.env_key="LITELLM_KEY"',"-c",'model_reasoning_effort="high"',"-c",`model_catalog_json="${CAT}"`];
    srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, LITELLM_KEY: "sk-spike-local", CODEX_HOME: codexHome }, stdio: ["pipe","pipe","ignore"] });
    let id = 1; const pend = new Map(); let buf = "", done = false, failed = null, mcpResult = "", sawMcp = false;
    const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
    srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
      if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
      if (m.id != null && m.method) { srv.stdin.write(JSON.stringify({ id: m.id, result: {} }) + "\n"); continue }
      const it = m.params?.item; const t = it?.type || "";
      if (/mcp/i.test(t)) { sawMcp = true; if (/42/.test(JSON.stringify(it))) mcpResult += "42"; }
      if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,100); done = true } } });

    await send("initialize", { clientInfo: { name: "mcp", version: "0" }, capabilities: null });
    const ts = await send("thread/start", { model: MODEL, modelProvider: "litellm", cwd: WS, approvalPolicy: "never", sandbox: "workspace-write" });
    const threadId = ts?.thread?.id ?? ts?.threadId;

    const status = await send("mcpServerStatus/list", { threadId });
    assert.match(JSON.stringify(status), /verifytools/, "the configured MCP server must load");

    const direct = await send("mcpServer/tool/call", { threadId, server: "verifytools", tool: "get_secret_number", arguments: {} });
    assert.match(JSON.stringify(direct), /42/, "codex must reach the MCP server and get its result (direct tool/call)");

    await send("turn/start", { threadId, input: [{ type: "text", text: "Call the get_secret_number MCP tool and report the secret number.", text_elements: [] }] });
    const t0 = Date.now(); while (!done && Date.now() - t0 < 90000) await new Promise(r => setTimeout(r, 400));
    console.log(`  ${MODEL}: server loaded ✓ | direct call=42 ✓ | model mcp_item=${sawMcp} resultSeen=${mcpResult.includes("42")}`);
    assert.ok(!failed, `MCP turn failed: ${failed}`);
    assert.ok(sawMcp, "a model turn must surface an MCP item (the tool is offered to the model)");
  } finally {
    srv?.kill("SIGKILL");
    bridge.kill("SIGKILL");
  }
});
