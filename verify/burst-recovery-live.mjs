// Can-fail proof that the resilience primitive recovers a REAL failing turn end-to-end (not a unit test
// with fake failures). A flaky proxy sits between codex and the bridge and returns 503 for an initial
// window — long enough to exhaust codex's internal retries so the turn genuinely THROWS. The façade's
// runResilient must retry past the window and succeed; plain run() throws. Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { CodexRuntime } from "../src/runtime.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const BRIDGE_PORT = 4034, PROXY_PORT = 4035;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const FAIL_WINDOW_MS = 12000; // 503 for the first 12s -> codex's internal retries exhaust -> turn throws

function startBridge() {
  const fd = openSync("/tmp/burst-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(BRIDGE_PORT)], {
    cwd: ROOT, env: { ...process.env }, stdio: ["ignore", fd, fd] });
}
async function health(port, ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${port}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

// flaky proxy: 503 during the fail window, then transparently pipe to the bridge
function startFlakyProxy(startTime) {
  return createServer((creq, cres) => {
    if (Date.now() - startTime < FAIL_WINDOW_MS) { cres.writeHead(503, { "Content-Type": "application/json" }); cres.end('{"error":"injected throttle"}'); return; }
    const preq = httpRequest({ host: "localhost", port: BRIDGE_PORT, path: creq.url, method: creq.method, headers: creq.headers },
      (pres) => { cres.writeHead(pres.statusCode || 502, pres.headers); pres.pipe(cres); });
    preq.on("error", () => { if (!cres.headersSent) cres.writeHead(502); cres.end(); });
    creq.pipe(preq);
  }).listen(PROXY_PORT);
}

test(`resilience recovers a real failing turn end-to-end (${MODEL})`, { timeout: 240000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  let proxy;
  try {
    assert.equal(await health(BRIDGE_PORT, 60000), true, "bridge must serve");
    const t0 = Date.now();
    proxy = startFlakyProxy(t0);
    const wd = mkdtempSync(join(tmpdir(), "burst-"));
    const rt = new CodexRuntime({ baseUrl: `http://localhost:${PROXY_PORT}/v1`, apiKey: "sk-spike-local", model: MODEL });
    const session = rt.startSession({ workingDirectory: wd, approvalPolicy: "never" });

    // resilient run: backoff must outlast the 12s fail window. First attempt(s) throw (proxy 503),
    // policy backs off + retries, by then the proxy is healthy -> succeeds.
    const FAST = { attemptTimeoutMs: 120000, maxAttempts: 5, initialBackoffMs: 4000, maxBackoffMs: 12000 };
    const result = await session.runResilient("Reply with exactly the word RECOVERED.", FAST);
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`  recovered after ${elapsed}s (fail window was ${FAIL_WINDOW_MS/1000}s) | reply=${JSON.stringify(result.finalResponse.slice(0,40))}`);
    assert.ok(Date.now() - t0 > FAIL_WINDOW_MS, "must have actually retried PAST the fail window (proves real recovery, not a lucky first try)");
    assert.match(result.finalResponse, /RECOVERED/i, "resilient turn must ultimately succeed after the injected failures");
  } finally {
    proxy?.close();
    bridge.kill("SIGKILL");
  }
});
