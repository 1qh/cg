// Can-fail proof that the bridge strict self-check is REAL (a green that could have been red).
// GREEN: patched proxy starts -> /health serves (all critical patches applied).
// RED:   LITELLM_DISABLE_CODEX_PATCH=1 -> self-check finds patches missing -> os._exit(97), never serves.
// The self-check fires at proxy startup, BEFORE any model call, so this runs on a DUMMY key — zero API spend.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn as spawnProc } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { readFileSync, writeFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4019; // isolated from the dev bridge on 4011

function startBridge(extraEnv) {
  return spawnProc("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT,
    env: { ...process.env, GEMINI_API_KEY: "dummy-selfcheck-no-call", LITELLM_PATCH_STRICT: "1", ...extraEnv },
    stdio: ["ignore", "ignore", "ignore"],
  });
}

// Faithful sabotage: the real patch file with ONE critical patch call dropped, self-check still runs —
// exactly the "a litellm change silently no-oped a patch" failure mode the self-check exists to catch.
function startBridgeWithDroppedPatch() {
  const real = readFileSync(join(ROOT, "litellm_patch", "sitecustomize.py"), "utf8");
  const sabotaged = real.replace(/^    _apply\(\)\n/m, "    # _apply() dropped to simulate a silent no-op\n");
  assert.notEqual(sabotaged, real, "sabotage must actually remove the _apply() call");
  const dir = mkdtempSync(join(tmpdir(), "bridge-sabotage-"));
  writeFileSync(join(dir, "sitecustomize.py"), sabotaged);
  return spawnProc(join(ROOT, ".litellm-venv", "bin", "litellm"),
    ["--config", join(ROOT, "bridge", "litellm-config.yaml"), "--port", String(PORT)], {
    cwd: ROOT,
    env: { ...process.env, PYTHONPATH: dir, GEMINI_API_KEY: "dummy-selfcheck-no-call",
           LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" },
    stdio: ["ignore", "ignore", "ignore"],
  });
}

async function healthWithin(ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`http://localhost:${PORT}/health/liveliness`, { signal: AbortSignal.timeout(1500) });
      if (r.ok) return true;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

function exitWithin(proc, ms) {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve({ exited: false }), ms);
    proc.on("exit", (code) => { clearTimeout(t); resolve({ exited: true, code }); });
  });
}

test("GREEN: patched bridge serves health (all critical patches applied)", async () => {
  const proc = startBridge({});
  try {
    const served = await healthWithin(60000);
    assert.equal(served, true, "patched bridge must serve /health within 60s");
  } finally {
    proc.kill("SIGKILL");
  }
});

test("RED: a critical patch silently no-ops -> strict self-check hard-exits 97, never serves", async () => {
  const proc = startBridgeWithDroppedPatch();
  const [served, ended] = await Promise.all([healthWithin(45000), exitWithin(proc, 45000)]);
  proc.kill("SIGKILL");
  assert.equal(served, false, "bridge with a missing critical patch must NOT serve under STRICT");
  assert.equal(ended.exited, true, "strict self-check must terminate the process");
  assert.equal(ended.code, 97, "hard-exit code must be 97 (the self-check sentinel)");
});
