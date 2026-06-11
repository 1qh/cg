// Can-fail proof of multimodal IMAGE INPUT through the harness: codex exec --image a solid-red PNG,
// assert the model identifies the color. Generates the PNG with stdlib (no image deps). Requires GEMINI_API_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync, openSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PORT = 4020;
const MODEL = process.env.PARITY_MODEL || "gemini-3.5-flash";
const CAT = join(ROOT, "bridge", "gemini-catalog.json");

function redPng(path) {
  // build a valid 8x8 solid-red PNG via python stdlib (zlib+struct) — no image library needed
  const py = `
import zlib,struct,sys
w=h=8
raw=b''.join(b'\\x00'+b'\\xff\\x00\\x00'*w for _ in range(h))
def chunk(t,d):
    c=t+d; return struct.pack('>I',len(d))+c+struct.pack('>I',zlib.crc32(c)&0xffffffff)
png=b'\\x89PNG\\r\\n\\x1a\\n'
png+=chunk(b'IHDR',struct.pack('>IIBBBBB',w,h,8,2,0,0,0))
png+=chunk(b'IDAT',zlib.compress(raw,9))
png+=chunk(b'IEND',b'')
open(sys.argv[1],'wb').write(png)
`;
  execFileSync("python3", ["-c", py, path]);
}

function startBridge() {
  const fd = openSync("/tmp/image-input-bridge.log", "w");
  return spawn("scripts/bridge.sh", ["run", String(PORT)], {
    cwd: ROOT, env: { ...process.env }, stdio: ["ignore", fd, fd] });
}
async function health(ms){const e=Date.now()+ms;while(Date.now()<e){try{if((await fetch(`http://localhost:${PORT}/health/liveliness`,{signal:AbortSignal.timeout(1500)})).ok)return true}catch{}await new Promise(r=>setTimeout(r,500))}return false}

test(`image input (multimodal) through the harness (${MODEL})`, { timeout: 180000 }, async () => {
  assert.ok(process.env.GEMINI_API_KEY, "GEMINI_API_KEY required");
  const bridge = startBridge();
  try {
    assert.equal(await health(60000), true, "bridge must serve");
    const wd = mkdtempSync(join(tmpdir(), "image-"));
    const img = join(wd, "red.png");
    redPng(img);
    const out = await new Promise((resolve) => {
      const p = spawn("codex", ["exec", "--image", img, "--skip-git-repo-check", "-C", wd,
        "-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
        "-c",`model_providers.gemini.base_url="http://localhost:${PORT}/v1"`,"-c",'model_providers.gemini.wire_api="responses"',
        "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",`model="${MODEL}"`,"-c",`model_catalog_json="${CAT}"`],
        { env: { ...process.env, BRIDGE_KEY: "sk-spike-local" }, stdio: ["pipe","pipe","ignore"] });
      let o = ""; p.stdout.on("data", d => o += d);
      p.stdin.write("What is the dominant color of this image? Answer with ONLY the color name.\n"); p.stdin.end();
      const t = setTimeout(() => { p.kill("SIGKILL"); resolve(o); }, 120000);
      p.on("exit", () => { clearTimeout(t); resolve(o); });
    });
    console.log(`  ${MODEL}: exec --image output tail=${JSON.stringify(out.slice(-80))}`);
    assert.match(out, /red/i, "model must identify the red image (multimodal image input works)");
  } finally {
    bridge.kill("SIGKILL");
  }
});
