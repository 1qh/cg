// Can-fail proof of the FULL product surface through the app-server façade (AppServerSession): the
// capabilities the typed SDK can't reach — goals, fork, interrupt — plus run. Closes the deferred
// façade gap: these were only ever proven via raw app-server, now they go through the product API.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { AppServerSession } from "../src/app-server-session.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4036;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/appserver-session-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`full product surface via AppServerSession (${MODEL})`, { timeout: 200000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  let s;
  const results = []; const ck = (n, p, d = "") => results.push({ n, p: !!p, d });
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const wd = mkdtempSync(join(tmpdir(), "appsess-"));
    execSync("git init -q && git commit -q --allow-empty -m i", { cwd: wd, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
    const opts = { workingDirectory: wd, approvalPolicy: "never", modelCatalogPath: CAT };
    s = new AppServerSession({ baseUrl: `http://localhost:${PORT}/v1`, apiKey: "sk-spike-local", model: MODEL }, opts);
    await s.start(opts);

    const r1 = await s.run("Reply with exactly: SURFACE_OK");
    ck("run via façade", r1.ok && /SURFACE_OK/.test(r1.message), r1.message.slice(0, 30));

    const g = await s.setGoal("Ship the widget", 50000);
    const gg = await s.getGoal();
    ck("goals set+get via façade", !g?.__error && JSON.stringify(gg).includes("Ship the widget"));

    const parentTid = s.threadId;
    const fk = await s.fork();
    const forkedTid = fk?.thread?.id ?? fk?.threadId;
    // red-capable: a real fork yields a NEW thread id distinct from the parent (not an echo / same thread)
    ck("fork via façade creates a distinct thread", !fk?.__error && (JSON.stringify(fk).includes("forkedFrom") || (!!forkedTid && forkedTid !== parentTid)), `forked=${forkedTid} parent=${parentTid}`);

    // interrupt: start a long turn, interrupt it, expect it to end promptly
    s.startTurnAsync("Run the shell command: sleep 30. Then say DONE.");
    await new Promise(r => setTimeout(r, 4000));
    const ab = await s.interrupt();
    const t0 = Date.now(); const at = await s.awaitTurn(20000);
    // red-capable: the interrupt must cut the turn before its sleep+DONE, so DONE never appears in the result
    ck("interrupt via façade cuts the turn before DONE", !ab?.__error && (Date.now() - t0) < 20000 && !/DONE/.test(JSON.stringify(at ?? "")), /DONE/.test(JSON.stringify(at ?? "")) ? "completed naturally" : "cut");

    const passed = results.filter(r => r.p).length;
    for (const r of results) console.log(`  ${r.p ? "PASS" : "FAIL"} ${r.n}${r.d ? " ("+r.d+")" : ""}`);
    console.log(`  ${MODEL}: ${passed}/${results.length} full-surface checks`);
    assert.equal(passed, results.length, `façade-surface failures: ${results.filter(r => !r.p).map(r => r.n).join(", ")}`);
  } finally {
    s?.close();
    bridge.kill("SIGKILL");
  }
});
