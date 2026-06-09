// Can-fail parity proof on the REAL path: codex -> patched bridge (grounding on) -> Gemini 3.
// Asserts the two load-bearing capabilities — multi-step tool-call coding (proves thought_signature handling)
// and web grounding (the union differentiator). Requires GEMINI_API_KEY. One model (flash) by default;
// set PARITY_MODEL to test another tier. Single short tasks — minimal API spend.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, existsSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4014;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/parity-live-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT,
    env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1", LITELLM_INJECT_GROUNDING: "1" },
    stdio: ["ignore", fd, fd],
  });
}
async function health(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    try { if ((await fetch(`http://localhost:${PORT}/health/liveliness`, { signal: AbortSignal.timeout(1500) })).ok) return true; } catch {}
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}
function appServer(wd) {
  const cfg = ["-c","model_provider=litellm","-c",'model_providers.litellm.name="g"',
    "-c",`model_providers.litellm.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.litellm.wire_api="responses"',
    "-c",'model_providers.litellm.env_key="LITELLM_KEY"',"-c",'model_reasoning_effort="high"',"-c",`model_catalog_json="${CAT}"`];
  const srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, LITELLM_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
  let id = 1; const pend = new Map(); let buf = "", done = false, failed = null, msg = "";
  const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
  srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
    if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
    if (m.id != null && m.method) { srv.stdin.write(JSON.stringify({ id: m.id, result: {} }) + "\n"); continue }
    const it = m.params?.item; if (it?.type === "agent_message") msg = (it.text || msg);
    if (m.method === "item/agentMessage/delta") msg += (m.params?.delta || "");
    if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,100); done = true } } });
  return { send, kill: () => srv.kill("SIGKILL"), state: () => ({ done, failed, msg }),
    reset: () => { done = false; failed = null; msg = ""; },
    async run(threadId, text, ms) { this.reset(); await send("turn/start", { threadId, input: [{ type: "text", text, text_elements: [] }] });
      const t0 = Date.now(); while (!this.state().done && Date.now() - t0 < ms) await new Promise(r => setTimeout(r, 400)); return this.state(); } };
}

test(`parity on ${MODEL}: multi-step coding + grounding via the real bridge`, { timeout: 300000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  let api;
  try {
    assert.equal(await health(60000), true, "patched bridge must serve");
    const wd = mkdtempSync(join(tmpdir(), "parity-live-"));
    execSync("git init -q && git commit -q --allow-empty -m i", { cwd: wd, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
    api = appServer(wd);
    await api.send("initialize", { clientInfo: { name: "pl", version: "0" }, capabilities: null });
    const ts = await api.send("thread/start", { model: MODEL, modelProvider: "litellm", cwd: wd, approvalPolicy: "never", sandbox: "workspace-write" });
    const threadId = ts?.thread?.id ?? ts?.threadId;

    const code = await api.run(threadId, "Create three files via separate shell commands one at a time: 'one'>a.txt, then 'two'>b.txt, then 'three'>c.txt. Then stop.", 150000);
    const files = ["a.txt","b.txt","c.txt"].filter(f => existsSync(join(wd, f))).length;
    assert.ok(!code.failed, `coding turn failed: ${code.failed}`);
    assert.equal(files, 3, `multi-step tool loop must create all 3 files (got ${files}) — proves thought_signature round-trip`);

    const grd = await api.run(threadId, "Who won the most recent Formula 1 Grand Prix? Use web search, then state the winner's name only.", 120000);
    assert.ok(!grd.failed, `grounding turn failed: ${grd.failed}`);
    assert.ok((grd.msg || "").trim().length > 0, "grounding turn must produce a non-empty answer (web grounding path)");
    console.log(`  ${MODEL}: 3/3 files | grounding answer=${JSON.stringify((grd.msg || "").slice(0, 60))}`);
  } finally {
    api?.kill();
    bridge.kill("SIGKILL");
  }
});
