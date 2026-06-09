// LIVE proof the typed thread façade drives the BYOK model end-to-end through the bridge.
// Spawns the patched bridge, opens a session in a throwaway git workdir, runs ONE turn that must edit a
// file, and asserts the edit landed. Single turn = minimal API spend. Requires GEMINI_API_KEY in env.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { CodexRuntime } from "../src/runtime.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4011;

function startBridge() {
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT,
    env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" },
    stdio: ["ignore", "ignore", "ignore"],
  });
}
async function healthWithin(ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    try { if ((await fetch(`http://localhost:${PORT}/health/liveliness`, { signal: AbortSignal.timeout(1500) })).ok) return true; }
    catch { /* not up */ }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

test("LIVE: façade edits a file on the BYOK model via the bridge", { timeout: 180000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY must be set");
  const bridge = startBridge();
  try {
    assert.equal(await healthWithin(60000), true, "bridge must serve health within 60s");
    const wd = mkdtempSync(join(tmpdir(), "runtime-live-"));
    execSync('git init -q && git commit -q --allow-empty -m i', { cwd: wd, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
    writeFileSync(join(wd, "math.py"), 'def add(a, b):\n    return a + b\n\nif __name__ == "__main__":\n    print(add(1, 2))\n');

    const rt = new CodexRuntime({ baseUrl: `http://localhost:${PORT}/v1`, apiKey: "sk-spike-local", model: "gemini-3.5-flash" });
    const session = rt.startSession({ workingDirectory: wd, approvalPolicy: "never" });
    const turn = await session.run("Add a multiply(a, b) function returning a*b to math.py. Edit the file directly.");

    const after = readFileSync(join(wd, "math.py"), "utf8");
    console.log("  thread:", session.id, "| finalResponse:", turn.finalResponse.slice(0, 60));
    assert.match(after, /def multiply\(a, b\)/, "façade must have driven the model to add multiply() to the file");
  } finally {
    bridge.kill("SIGKILL");
  }
});
