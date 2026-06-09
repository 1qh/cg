// Can-fail proof that the system absorbs a REAL sustained burst: fire N concurrent turns at once (genuine
// rate pressure on the model), each via the façade's runResilient. The spike found turn-completion degrades
// under burst; this asserts resilience recovers it — all concurrent turns ultimately complete. GEMINI_API_KEY req.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { CodexRuntime } from "../src/runtime.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4037;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CONCURRENCY = Number(process.env.BURST || "6");

function startBridge() {
  const fd = openSync("/tmp/burst-sustained-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`sustained burst: ${CONCURRENCY} concurrent resilient turns all complete (${MODEL})`, { timeout: 420000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const rt = new CodexRuntime({ baseUrl: `http://localhost:${PORT}/v1`, apiKey: "sk-spike-local", model: MODEL });
    const RES = { attemptTimeoutMs: 180000, maxAttempts: 5, initialBackoffMs: 3000, maxBackoffMs: 30000 };

    // fire CONCURRENCY turns AT ONCE (real burst), each resilient
    const jobs = Array.from({ length: CONCURRENCY }, (_, i) => (async () => {
      const wd = mkdtempSync(join(tmpdir(), `burst-${i}-`));
      execSync("git init -q && git commit -q --allow-empty -m i", { cwd: wd, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
      const session = rt.startSession({ workingDirectory: wd, approvalPolicy: "never" });
      try {
        const r = await session.runResilient(`Reply with exactly: BURST_OK_${i}`, RES);
        return { i, ok: new RegExp(`BURST_OK_${i}`).test(r.finalResponse) };
      } catch (e) { return { i, ok: false, err: String(e).slice(0, 60) }; }
    })());

    const t0 = Date.now();
    const results = await Promise.all(jobs);
    const ok = results.filter(r => r.ok).length;
    const secs = ((Date.now() - t0) / 1000).toFixed(1);
    for (const r of results.filter(r => !r.ok)) console.log(`  turn ${r.i} FAILED: ${r.err || "wrong reply"}`);
    console.log(`  ${MODEL}: ${ok}/${CONCURRENCY} concurrent turns completed in ${secs}s (resilience absorbing burst)`);
    assert.equal(ok, CONCURRENCY, `all ${CONCURRENCY} concurrent turns must complete via resilience (got ${ok})`);
  } finally {
    bridge.kill("SIGKILL");
  }
});
