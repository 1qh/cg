// Can-fail proof that Gemini's native reasoning SURFACES through the harness (a union capability).
// Needs summaries-on catalog + model_reasoning_summary set + the reasoning-summary SSE patch. Asserts a
// real `reasoning` item with non-empty content appears in the app-server event stream. Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4016;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/reasoning-live-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`Gemini reasoning surfaces through the harness (${MODEL})`, { timeout: 200000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  let srv;
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const wd = mkdtempSync(join(tmpdir(), "reasoning-"));
    execSync("git init -q && git commit -q --allow-empty -m i", { cwd: wd, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
    const cfg = ["-c","model_provider=litellm","-c",'model_providers.litellm.name="g"',
      "-c",`model_providers.litellm.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.litellm.wire_api="responses"',
      "-c",'model_providers.litellm.env_key="LITELLM_KEY"',"-c",'model_reasoning_effort="high"',"-c",'model_reasoning_summary="detailed"',"-c",`model_catalog_json="${CAT}"`];
    srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, LITELLM_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
    let id = 1; const pend = new Map(); let buf = "", done = false, failed = null, reasoningText = "";
    const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
    srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
      if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
      if (m.id != null && m.method) { srv.stdin.write(JSON.stringify({ id: m.id, result: {} }) + "\n"); continue }
      const it = m.params?.item;
      if (it && /reason/i.test(it.type || "")) { const t = it.text || (Array.isArray(it.summary) ? it.summary.join(" ") : "") || JSON.stringify(it).slice(0,0); if (t) reasoningText += t; }
      if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,100); done = true } } });

    await send("initialize", { clientInfo: { name: "rs", version: "0" }, capabilities: null });
    const ts = await send("thread/start", { model: MODEL, modelProvider: "litellm", cwd: wd, approvalPolicy: "never", sandbox: "workspace-write" });
    const threadId = ts?.thread?.id ?? ts?.threadId;
    await send("turn/start", { threadId, input: [{ type: "text", text: "Think step by step about why the sky appears blue, then give a one-sentence answer.", text_elements: [] }] });
    const t0 = Date.now(); while (!done && Date.now() - t0 < 120000) await new Promise(r => setTimeout(r, 400));
    assert.ok(!failed, `turn failed: ${failed}`);
    assert.ok(done, "turn must complete (not hang) — proves the reasoning-summary SSE patch works");
    assert.ok(reasoningText.trim().length > 0, "a non-empty reasoning item must surface (native CoT through the harness)");
    console.log(`  ${MODEL}: reasoning surfaced (${reasoningText.length} chars): ${JSON.stringify(reasoningText.slice(0,60))}`);
  } finally {
    srv?.kill("SIGKILL");
    bridge.kill("SIGKILL");
  }
});
