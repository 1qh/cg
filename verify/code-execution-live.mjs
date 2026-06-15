// Can-fail proof of gemini server-side CODE EXECUTION through the harness: codex exec a tedious
// exact-computation task (sum of all primes below 1000 = 76127) that the model reliably nails only
// by running code, not mental arithmetic. The bridge injects GTool::code_execution alongside
// grounding + function tools. Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4021;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/code-execution-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`code execution (server-side) through the harness (${MODEL})`, { timeout: 180000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const wd = mkdtempSync(join(tmpdir(), "codeexec-"));
    const out = await new Promise((resolve) => {
      const p = spawn("codex", ["exec", "--skip-git-repo-check", "-C", wd,
        "-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
        "-c",`model_providers.gemini.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.gemini.wire_api="responses"',
        "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",`model="${MODEL}"`,"-c",`model_catalog_json="${CAT}"`],
        { env: { ...process.env, BRIDGE_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
      let o = ""; p.stdout.on("data", d => o += d);
      p.stdin.write("Run code to compute the exact sum of all prime numbers below 1000. State only the final integer.\n"); p.stdin.end();
      const t = setTimeout(() => { p.kill("SIGKILL"); resolve(o); }, 120000);
      p.on("exit", () => { clearTimeout(t); resolve(o); });
    });
    console.log(`  ${MODEL}: exec output tail=${JSON.stringify(out.slice(-100))}`);
    assert.match(out, /76127/, "model must reach 76127 via code execution (server-side code_execution works)");
  } finally {
    bridge.kill("SIGKILL");
  }
});
