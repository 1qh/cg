// Can-fail proof of app-layer compaction (the ADR's claimed continuity assertion), on the real path.
// Plant facts in thread A -> ask A for a handoff summary -> start a FRESH thread B seeded with the summary ->
// assert B recalls the planted facts. Proves the structural context reset carries continuity. Requires
// GEMINI_API_KEY. Uses the typed façade (also exercises src/runtime.ts).
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { CodexRuntime } from "../src/runtime.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4015;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";

function startBridge() {
  const fd = openSync("/tmp/compaction-live-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" },
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

test(`app-layer compaction carries continuity across a fresh thread (${MODEL})`, { timeout: 240000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const wd = mkdtempSync(join(tmpdir(), "compaction-"));
    const rt = new CodexRuntime({ baseUrl: `http://localhost:${PORT}/v1`, apiKey: "sk-spike-local", model: MODEL });

    // Thread A: plant three facts.
    const a = rt.startSession({ workingDirectory: wd, approvalPolicy: "never" });
    await a.run("Remember these three facts for later: the project codeword is FALCON, the port is 8472, the owner is Mira. Acknowledge briefly.");
    const summaryTurn = await a.run("Produce a concise handoff summary of everything I asked you to remember, as plain text I can paste into a fresh session.");
    const summary = summaryTurn.finalResponse;
    assert.ok(/FALCON/i.test(summary) && /8472/.test(summary) && /Mira/i.test(summary), `handoff summary must carry the facts; got: ${summary.slice(0,120)}`);

    // Thread B: fresh, seeded with the summary only.
    const b = rt.startSession({ workingDirectory: wd, approvalPolicy: "never" });
    const recall = await b.run(`Context from a prior session:\n${summary}\n\nUsing only that context, what are the codeword, the port, and the owner? Answer as: codeword=..., port=..., owner=...`);
    const r = recall.finalResponse;
    assert.match(r, /FALCON/i, "fresh thread must recall the codeword from the seeded summary");
    assert.match(r, /8472/, "fresh thread must recall the port");
    assert.match(r, /Mira/i, "fresh thread must recall the owner");
    console.log(`  ${MODEL}: continuity carried — recall=${JSON.stringify(r.slice(0, 70))}`);
  } finally {
    bridge.kill("SIGKILL");
  }
});
