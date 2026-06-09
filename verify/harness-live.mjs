// Comprehensive can-fail harness suite on the CURRENT stack (codex 0.138, litellm 1.88.1 + patches,
// 3-tier catalog). Exercises every Codex harness capability the adapter must preserve, each as a can-fail
// check, against the app-server. Grounding is verified separately (needs the injection env). Requires
// GEMINI_API_KEY. PARITY_MODEL overrides the tier (default flash).
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4017;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/harness-live-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env, LITELLM_MASTER_KEY: "sk-spike-local", LITELLM_PATCH_STRICT: "1" }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`harness capability suite (${MODEL})`, { timeout: 480000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  const WS = mkdtempSync(join(tmpdir(), "harness-"));
  execSync("git init -q && git commit -q --allow-empty -m i", { cwd: WS, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
  let srv;
  const results = [];
  const ck = (name, pass, detail = "") => results.push({ name, pass: !!pass, detail });
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const cfg = ["-c","model_provider=litellm","-c",'model_providers.litellm.name="g"',
      "-c",`model_providers.litellm.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.litellm.wire_api="responses"',
      "-c",'model_providers.litellm.env_key="LITELLM_KEY"',"-c",'model_reasoning_effort="high"',"-c",'model_reasoning_summary="detailed"',"-c",`model_catalog_json="${CAT}"`];
    srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, LITELLM_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","pipe"] });
    let id = 1; const pend = new Map(); let buf = "", done = false, failed = null, msg = "", reasoning = 0, shell = 0, activeTurn = null, stderrErrs = 0;
    const notifs = new Set();
    srv.stderr?.on?.("data", d => { if (/OutputTextDelta without active item/.test(d.toString())) stderrErrs++; });
    const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
    srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
      if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
      if (m.id != null && m.method) { srv.stdin.write(JSON.stringify({ id: m.id, result: m.method.toLowerCase().includes("approval") ? { decision: "accept" } : {} }) + "\n"); continue }
      if (m.method) notifs.add(m.method);
      if (m.method === "turn/started") activeTurn = m.params?.turn?.id ?? m.params?.turnId ?? m.params?.id;
      const it = m.params?.item; const t = it?.type || "";
      if (/reason/i.test(t)) reasoning++;
      if (t === "command_execution" || /command/i.test(t)) shell++;
      if (t === "agent_message") msg = (it.text || msg);
      if (m.method === "item/agentMessage/delta") msg += (m.params?.delta || "");
      if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,100); done = true } } });

    const newThread = async () => { const ts = await send("thread/start", { model: MODEL, modelProvider: "litellm", cwd: WS, approvalPolicy: "never", sandbox: "workspace-write" }); return ts?.thread?.id ?? ts?.threadId; };
    const turn = async (tid, text, ms = 120000) => { done = false; failed = null; msg = ""; reasoning = 0; shell = 0; await send("turn/start", { threadId: tid, input: [{ type: "text", text, text_elements: [] }] }); const t0 = Date.now(); while (!done && Date.now() - t0 < ms) await new Promise(r => setTimeout(r, 300)); return { ok: done && !failed, failed, msg, reasoning, shell }; };

    await send("initialize", { clientInfo: { name: "h", version: "0" }, capabilities: null });

    const A = await newThread();
    const t1 = await turn(A, "Think step by step about whether 17 is a prime number and why, then reply with exactly: ALPHA_OK");
    ck("turn completes", t1.ok, t1.failed || "");
    ck("message captured", /ALPHA_OK/.test(t1.msg), t1.msg.slice(0,40));
    ck("reasoning surfaces", t1.reasoning > 0, `${t1.reasoning} items`);
    ck("streaming deltas", notifs.has("item/agentMessage/delta"));
    ck("token usage", notifs.has("thread/tokenUsage/updated"));
    ck("no OutputTextDelta errors", stderrErrs === 0, `${stderrErrs}`);

    const t2 = await turn(A, "Run the shell command: echo SHELL_OK_42. Then tell me what it printed.");
    ck("shell tool executes", t2.shell > 0, `${t2.shell}`);
    ck("shell output reported", /SHELL_OK_42/.test(t2.msg));

    await turn(A, "Remember the number 31337.");
    const t3 = await turn(A, "What number did I just ask you to remember? Reply with only the number.");
    ck("multi-turn continuity", /31337/.test(t3.msg), t3.msg.slice(0,30));

    const t4 = await turn(A, "Create a file called out.txt containing the word DONE_WRITING using a shell command.");
    ck("agentic file write (apply_patch via shell)", t4.ok && existsSync(join(WS, "out.txt")) && /DONE_WRITING/.test(readFileSync(join(WS, "out.txt"), "utf8")));

    // CLEAN read-style checks first (structured output, compaction); state-MUTATING checks (goals/steer) run
    // LAST so they cannot bleed into the others — shared-session state pollution caused earlier false reds.
    const B = await newThread();
    done = false; failed = null; msg = "";
    await send("turn/start", { threadId: B, input: [{ type: "text", text: "Do NOT use any tools. Immediately output ONLY the JSON object for a person named Alice aged 30.", text_elements: [] }], outputSchema: { type: "object", properties: { name: { type: "string" }, age: { type: "integer" } }, required: ["name", "age"], additionalProperties: false } });
    { const t0 = Date.now(); while (!done && Date.now() - t0 < 60000) await new Promise(r => setTimeout(r, 300)); }
    let sj = null; try { sj = JSON.parse(msg.match(/\{[\s\S]*\}/)?.[0] || "null") } catch {}
    ck("structured output (outputSchema)", sj && sj.name && typeof sj.age === "number", msg.slice(0,50));

    const C = await newThread();
    await turn(C, "Remember: codeword QUARTZ9, owner is Sam.");
    const sum = await turn(C, "Produce a handoff summary with every fact for a fresh session. Output only the summary.");
    const D = await newThread();
    await turn(D, `Resume. Handoff summary:\n${sum.msg}\nAcknowledge in one line.`);
    const rec = await turn(D, "From the loaded context: what is the codeword and owner? Reply 'codeword, owner'.");
    ck("app-layer compaction continuity", /QUARTZ9/i.test(rec.msg) && /Sam/i.test(rec.msg), rec.msg.slice(0,40));

    // state-MUTATING checks LAST (own fresh thread) — goals/fork/steer
    const M = await newThread();
    const g = await send("thread/goal/set", { threadId: M, objective: "Ship the widget", tokenBudget: 50000 });
    const gg = await send("thread/goal/get", { threadId: M });
    ck("goals set+get", !g?.__e && JSON.stringify(gg).includes("Ship the widget"));

    const fk = await send("thread/fork", { threadId: A }); // fork a thread WITH turn history, not a turnless one
    ck("fork", !fk?.__e && (JSON.stringify(fk).includes("forkedFrom") || !!fk?.thread?.id));

    const E = await newThread();
    done = false; failed = null; msg = "";
    send("turn/start", { threadId: E, input: [{ type: "text", text: "Run the shell command `sleep 8`, then write a 4-line poem about rivers.", text_elements: [] }] });
    await new Promise(r => setTimeout(r, 3500));
    const sr = await send("turn/steer", { threadId: E, expectedTurnId: activeTurn, input: [{ type: "text", text: "CHANGE OF PLANS: no poem. After the sleep, output exactly the word KESTREL.", text_elements: [] }] });
    { const t0 = Date.now(); while (!done && Date.now() - t0 < 60000) await new Promise(r => setTimeout(r, 300)); }
    ck("steer accepted", !sr?.__e); // the steer CALL is deterministic
    // behavioral redirect is TIMING-SENSITIVE (best-effort): the model may already be past the steer point.
    // Retry the steer scenario a couple times before failing — reflects real best-effort steer, not a placebo.
    let redirected = /KESTREL/i.test(msg) && !/river/i.test(msg);
    for (let r = 0; r < 2 && !redirected; r++) {
      const E2 = await newThread(); done = false; failed = null; msg = "";
      send("turn/start", { threadId: E2, input: [{ type: "text", text: "Run the shell command `sleep 8`, then write a 4-line poem about rivers.", text_elements: [] }] });
      await new Promise(rr => setTimeout(rr, 3500));
      await send("turn/steer", { threadId: E2, expectedTurnId: activeTurn, input: [{ type: "text", text: "CHANGE OF PLANS: no poem. After the sleep, output exactly the word KESTREL.", text_elements: [] }] });
      const t0 = Date.now(); while (!done && Date.now() - t0 < 60000) await new Promise(rr => setTimeout(rr, 300));
      redirected = /KESTREL/i.test(msg) && !/river/i.test(msg);
    }
    ck("steer redirects behavior (timing-sensitive, ≤3 tries)", redirected, msg.slice(0,40));

    const passed = results.filter(r => r.pass).length;
    for (const r of results) console.log(`  ${r.pass ? "PASS" : "FAIL"} ${r.name}${r.detail ? " ("+r.detail+")" : ""}`);
    console.log(`  ${MODEL}: ${passed}/${results.length} harness checks`);
    assert.equal(passed, results.length, `harness failures: ${results.filter(r => !r.pass).map(r => r.name).join(", ")}`);
  } finally {
    srv?.kill("SIGKILL");
    bridge.kill("SIGKILL");
  }
});
