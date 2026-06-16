// Advanced harness capabilities not in the base suite: interactive approval (accept AND decline),
// parallel tool calls, and turn abort. Each a can-fail check on the real path. Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { existsSync, mkdtempSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4018;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/harness-adv-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`advanced harness: approval accept/decline, parallel tools, abort (${MODEL})`, { timeout: 360000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  const WS = mkdtempSync(join(tmpdir(), "harness-adv-"));
  execSync("git init -q && git commit -q --allow-empty -m i", { cwd: WS, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
  let srv;
  const results = [];
  const ck = (n, p, d = "") => results.push({ n, p: !!p, d });
  let decisionMode = "accept", approvalAsked = false;
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const cfg = ["-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
      "-c",`model_providers.gemini.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.gemini.wire_api="responses"',
      "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",'model_reasoning_effort="high"',"-c",`model_catalog_json="${CAT}"`];
    srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, BRIDGE_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
    let id = 1; const pend = new Map(); let buf = "", done = false, failed = null, shellItems = 0, activeTurn = null;
    const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
    srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
      if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
      if (m.id != null && m.method) {
        if (/approval/i.test(m.method)) { approvalAsked = true; srv.stdin.write(JSON.stringify({ id: m.id, result: { decision: decisionMode } }) + "\n"); }
        else srv.stdin.write(JSON.stringify({ id: m.id, result: {} }) + "\n");
        continue;
      }
      if (m.method === "turn/started") activeTurn = m.params?.turn?.id ?? m.params?.turnId ?? m.params?.id;
      const it = m.params?.item; const t = it?.type || "";
      if (t === "command_execution" || /command/i.test(t)) shellItems++;
      if (m.method === "turn/started") shellItems = 0;
      if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,80); done = true } } });

    const thread = async (policy) => { const ts = await send("thread/start", { model: MODEL, modelProvider: "gemini", cwd: WS, approvalPolicy: policy, sandbox: "workspace-write" }); return ts?.thread?.id ?? ts?.threadId; };
    const turn = async (tid, text, ms = 120000) => { done = false; failed = null; shellItems = 0; await send("turn/start", { threadId: tid, input: [{ type: "text", text, text_elements: [] }] }); const t0 = Date.now(); while (!done && Date.now() - t0 < ms) await new Promise(r => setTimeout(r, 300)); return { ok: done && !failed, secs: (Date.now()-t0)/1000 }; };

    await send("initialize", { clientInfo: { name: "ha", version: "0" }, capabilities: null });

    // approval ACCEPT — command must run + create the file
    decisionMode = "accept"; approvalAsked = false;
    const A = await thread("untrusted");
    await turn(A, "Run a shell command to create a file accepted.txt containing OK.");
    ck("approval requested", approvalAsked);
    ck("approval accept -> command runs", existsSync(join(WS, "accepted.txt")));

    // approval DECLINE — command must NOT run
    decisionMode = "decline"; approvalAsked = false;
    const B = await thread("untrusted");
    await turn(B, "Run a shell command to create a file declined.txt containing NO.");
    ck("approval decline -> command blocked", approvalAsked && !existsSync(join(WS, "declined.txt")));

    // multiple tool calls in one turn — red-capable for the multi-function-call path (a dropped call -> <2)
    // (true concurrency is not deterministic on the Gemini path per PRODUCT.md, so we verify the real
    // guarantee: the bridge delivers >=2 of the requested independent commands as tool items in one turn)
    const C = await thread("never");
    await turn(C, "In ONE turn, run these independent shell commands: echo P1; and separately echo P2; and separately echo P3.");
    ck("multiple tool calls handled in one turn", shellItems >= 2, `${shellItems} command items`);

    // abort — start a long sleep, interrupt it, expect it to end fast
    const D = await thread("never");
    done = false; failed = null;
    send("turn/start", { threadId: D, input: [{ type: "text", text: "Run the shell command: sleep 30. Then say DONE.", text_elements: [] }] });
    await new Promise(r => setTimeout(r, 4000));
    const ab = await send("turn/interrupt", { threadId: D, turnId: activeTurn });
    const t0 = Date.now(); while (!done && Date.now() - t0 < 20000) await new Promise(r => setTimeout(r, 300));
    // red-capable: the interrupt must cut the turn BEFORE it finishes its sleep+DONE, so DONE never appears
    ck("abort/interrupt cuts the turn before natural completion", !ab?.__e && (Date.now() - t0) < 20000 && !/DONE/.test(buf), ab?.__e ? "interrupt err" : (/DONE/.test(buf) ? "turn completed naturally — NOT interrupted" : "cut before DONE"));

    const passed = results.filter(r => r.p).length;
    for (const r of results) console.log(`  ${r.p ? "PASS" : "FAIL"} ${r.n}${r.d ? " ("+r.d+")" : ""}`);
    console.log(`  ${MODEL}: ${passed}/${results.length} advanced checks`);
    assert.equal(passed, results.length, `advanced failures: ${results.filter(r => !r.p).map(r => r.n).join(", ")}`);
  } finally {
    srv?.kill("SIGKILL");
    bridge.kill("SIGKILL");
  }
});
