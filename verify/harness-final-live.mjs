// Final harness checks: model listing, structured error / turn-failed surfacing, and OS sandbox
// (Seatbelt workspace-write) enforcement. Each a can-fail check. Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execSync } from "node:child_process";
import { existsSync, mkdtempSync, openSync, rmSync } from "node:fs";
import { tmpdir, homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4021;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function startBridge() {
  const fd = openSync("/tmp/harness-final-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`final harness: model list, error surfacing, sandbox enforcement (${MODEL})`, { timeout: 240000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  const WS = mkdtempSync(join(tmpdir(), "harness-final-"));
  execSync("git init -q && git commit -q --allow-empty -m i", { cwd: WS, env: { ...process.env, GIT_AUTHOR_NAME: "x", GIT_AUTHOR_EMAIL: "a@b.c", GIT_COMMITTER_NAME: "x", GIT_COMMITTER_EMAIL: "a@b.c" } });
  const escapePath = join(homedir(), `.codex_sandbox_probe_${process.pid}.txt`);
  let srv; const results = []; const ck = (n, p, d = "") => results.push({ n, p: !!p, d });
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const cfg = ["-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
      "-c",`model_providers.gemini.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.gemini.wire_api="responses"',
      "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",'model_reasoning_effort="high"',"-c",`model_catalog_json="${CAT}"`];
    srv = spawn("codex", ["app-server", ...cfg], { env: { ...process.env, BRIDGE_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
    let id = 1; const pend = new Map(); let buf = "", done = false, failed = null;
    const send = (m, pa) => { const i = id++; srv.stdin.write(JSON.stringify({ method: m, id: i, params: pa }) + "\n"); return new Promise(r => pend.set(i, r)); };
    srv.stdout.on("data", c => { buf += c; let nl; while ((nl = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1); if (!l) continue; let m; try { m = JSON.parse(l) } catch { continue }
      if (m.id != null && (m.result !== undefined || m.error !== undefined) && pend.has(m.id)) { pend.get(m.id)(m.result ?? { __e: m.error }); pend.delete(m.id); continue }
      if (m.id != null && m.method) { srv.stdin.write(JSON.stringify({ id: m.id, result: {} }) + "\n"); continue }
      if (m.method === "turn/completed") done = true; if (m.method === "turn/failed") { failed = JSON.stringify(m.params).slice(0,120); done = true } } });
    const turn = async (tid, text, ms = 120000) => { done = false; failed = null; await send("turn/start", { threadId: tid, input: [{ type: "text", text, text_elements: [] }] }); const t0 = Date.now(); while (!done && Date.now() - t0 < ms) await new Promise(r => setTimeout(r, 300)); return { ok: done && !failed, failed }; };

    await send("initialize", { clientInfo: { name: "hf", version: "0" }, capabilities: null });

    // model listing
    const ml = await send("model/list", {});
    ck("model/list returns models", !ml?.__e && JSON.stringify(ml).length > 5, JSON.stringify(ml).slice(0, 50));

    // structured error surfacing — a connection failure (bridge at a dead port) must FAIL cleanly, not hang
    const errOut = await new Promise((resolve) => {
      const p = spawn("codex", ["exec", "--skip-git-repo-check", "-C", WS,
        "-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
        "-c",'model_providers.gemini.base_url="http://localhost:59999/v1"',"-c",'model_providers.gemini.wire_api="responses"',
        "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",`model="${MODEL}"`,"-c",`model_catalog_json="${CAT}"`],
        { env: { ...process.env, BRIDGE_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","pipe"] });
      let o = ""; p.stdout.on("data", d => o += d); p.stderr.on("data", d => o += d);
      p.stdin.write("Say hi.\n"); p.stdin.end();
      let code = null; const t = setTimeout(() => { p.kill("SIGKILL"); resolve({ o, code, killed: true }); }, 45000);
      p.on("exit", (c) => { clearTimeout(t); resolve({ o, code: c, killed: false }); });
    });
    ck("error surfaces cleanly on connection failure (no hang)", !errOut.killed && /error|refus|connect|fail|stream/i.test(errOut.o), `exit=${errOut.code} ${errOut.o.slice(-60)}`);

    // OS sandbox enforcement — workspace-write must block a write OUTSIDE the workspace
    const S = await send("thread/start", { model: MODEL, modelProvider: "gemini", cwd: WS, approvalPolicy: "never", sandbox: "workspace-write" });
    const sid = S?.thread?.id ?? S?.threadId;
    await turn(sid, `Run a shell command that writes the word ESCAPED to the absolute path ${escapePath} (outside this workspace).`, 120000);
    ck("sandbox blocks out-of-workspace write", !existsSync(escapePath), escapePath);

    const passed = results.filter(r => r.p).length;
    for (const r of results) console.log(`  ${r.p ? "PASS" : "FAIL"} ${r.n}${r.d ? " ("+r.d+")" : ""}`);
    console.log(`  ${MODEL}: ${passed}/${results.length} final checks`);
    assert.equal(passed, results.length, `final failures: ${results.filter(r => !r.p).map(r => r.n).join(", ")}`);
  } finally {
    try { if (existsSync(escapePath)) rmSync(escapePath); } catch {}
    srv?.kill("SIGKILL");
    bridge.kill("SIGKILL");
  }
});
