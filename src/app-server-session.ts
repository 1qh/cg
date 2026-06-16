// App-server-backed session: the FULL product surface the typed SDK can't reach (goals, fork, steer,
// interrupt, resume) plus run/stream, over the codex app-server JSON-RPC. Fail-fast on missing config.
// Pairs with the SDK-based CodexRuntime (run/structured-output/resilient); this is the complete-surface path.
import { spawn, type ChildProcess } from "node:child_process";
import type { BridgeConfig, SandboxMode, ApprovalPolicy } from "./runtime.ts";

export interface AppServerSessionOptions {
  readonly workingDirectory: string;
  readonly sandboxMode?: SandboxMode;
  readonly approvalPolicy?: ApprovalPolicy;
  /** Path to the model-catalog JSON so codex recognizes the BYOK model. */
  readonly modelCatalogPath: string;
  readonly reasoningEffort?: string;
  readonly reasoningSummary?: string;
}

interface Pending { resolve: (v: unknown) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout>; }

/** One codex app-server process + thread, exposing the complete harness surface. Call close() when done. */
export class AppServerSession {
  readonly #proc: ChildProcess;
  #id = 1;
  readonly #pending = new Map<number, Pending>();
  #buf = "";
  #stderr = "";
  #threadId: string | null = null;
  #activeTurn: string | null = null;
  #done = false;
  #failed: string | null = null;
  #msg = "";
  #dead = false;
  #turnInFlight = false;
  readonly #model: string;
  readonly #sendTimeoutMs = 60_000;

  constructor(cfg: BridgeConfig, opts: AppServerSessionOptions) {
    for (const k of ["baseUrl", "apiKey", "model"] as const) if (!cfg[k]) throw new Error(`BridgeConfig.${k} required`);
    if (!opts.workingDirectory || !opts.modelCatalogPath) throw new Error("workingDirectory + modelCatalogPath required");
    this.#model = cfg.model;
    const c = ["-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
      "-c",`model_providers.gemini.base_url="${cfg.baseUrl}"`,"-c",'model_providers.gemini.wire_api="responses"',
      "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",`model_reasoning_effort="${opts.reasoningEffort ?? "high"}"`,
      ...(opts.reasoningSummary ? ["-c",`model_reasoning_summary="${opts.reasoningSummary}"`] : []),
      "-c",`model_catalog_json="${opts.modelCatalogPath}"`];
    this.#proc = spawn("codex", ["app-server", ...c], { env: { ...process.env, BRIDGE_KEY: cfg.apiKey }, stdio: ["pipe", "pipe", "pipe"] });
    this.#proc.stdout!.on("data", (chunk: Buffer) => this.#onData(chunk));
    this.#proc.stderr?.on("data", (chunk: Buffer) => { this.#stderr = (this.#stderr + chunk.toString()).slice(-4000); });
    this.#proc.on("exit", (code) => this.#die(`codex app-server exited (code ${code})${this.#stderr ? `: ${this.#stderr.slice(-300)}` : ""}`));
    this.#proc.on("error", (e) => this.#die(`codex app-server process error: ${e.message}`));
  }

  // Reject every in-flight request and mark the session dead — so no caller hangs on a crashed subprocess.
  #die(reason: string): void {
    if (this.#dead) return;
    this.#dead = true; this.#failed = reason; this.#done = true; this.#turnInFlight = false;
    for (const p of this.#pending.values()) { clearTimeout(p.timer); p.reject(new Error(reason)); }
    this.#pending.clear();
  }

  #onData(chunk: Buffer): void {
    this.#buf += chunk.toString();
    let nl: number;
    while ((nl = this.#buf.indexOf("\n")) >= 0) {
      const line = this.#buf.slice(0, nl).trim(); this.#buf = this.#buf.slice(nl + 1);
      if (!line) continue;
      let m: Record<string, unknown>;
      try { m = JSON.parse(line) as Record<string, unknown>; } catch { continue; }
      const id = m["id"] as number | undefined;
      if (id != null && (m["result"] !== undefined || m["error"] !== undefined) && this.#pending.has(id)) {
        const p = this.#pending.get(id)!; clearTimeout(p.timer); this.#pending.delete(id);
        p.resolve(m["result"] ?? { __error: m["error"] }); continue;
      }
      if (id != null && m["method"]) { this.#write({ id, result: {} }); continue; }
      const method = m["method"] as string | undefined;
      const params = m["params"] as Record<string, unknown> | undefined;
      if (method === "turn/started") this.#activeTurn = (params?.["turn"] as Record<string, unknown>)?.["id"] as string ?? (params?.["turnId"] as string) ?? null;
      const item = params?.["item"] as Record<string, unknown> | undefined;
      if (item?.["type"] === "agent_message") this.#msg = (item["text"] as string) || this.#msg;
      if (method === "item/agentMessage/delta") this.#msg += (params?.["delta"] as string) || "";
      if (method === "turn/completed") { this.#done = true; this.#turnInFlight = false; }
      if (method === "turn/failed") { this.#failed = JSON.stringify(params); this.#done = true; this.#turnInFlight = false; }
    }
  }

  // Guarded write: a write to a dead/closed stdin throws EPIPE — surface it, never crash the process.
  #write(obj: unknown): void {
    try { this.#proc.stdin!.write(JSON.stringify(obj) + "\n"); }
    catch (e) { this.#die(`app-server stdin write failed: ${(e as Error).message}`); }
  }

  // Every JSON-RPC call carries a deadline; a missing/never-arriving response rejects instead of hanging forever.
  #send(method: string, params: unknown): Promise<unknown> {
    const id = this.#id++;
    return new Promise((resolve, reject) => {
      if (this.#dead) { reject(new Error(`app-server dead: ${this.#failed ?? "closed"}`)); return; }
      const timer = setTimeout(() => {
        if (this.#pending.delete(id)) reject(new Error(`app-server request "${method}" timed out after ${this.#sendTimeoutMs}ms`));
      }, this.#sendTimeoutMs);
      this.#pending.set(id, { resolve, reject, timer });
      this.#write({ method, id, params });
    });
  }

  async start(opts: AppServerSessionOptions): Promise<void> {
    await this.#send("initialize", { clientInfo: { name: "codex-byok", version: "0" }, capabilities: null });
    const ts = await this.#send("thread/start", { model: this.#model, modelProvider: "gemini", cwd: opts.workingDirectory, approvalPolicy: opts.approvalPolicy ?? "on-request", sandbox: opts.sandboxMode ?? "workspace-write" }) as Record<string, unknown>;
    this.#threadId = (ts?.["thread"] as Record<string, unknown>)?.["id"] as string ?? (ts?.["threadId"] as string) ?? null;
    if (!this.#threadId) throw new Error("thread/start did not return a thread id");
  }

  get threadId(): string | null { return this.#threadId; }
  get activeTurnId(): string | null { return this.#activeTurn; }

  // Enforce one turn at a time: the single-scalar turn state cannot represent overlapping turns, so a
  // concurrent start fails fast (a wrong-turn steer/interrupt or clobbered completion would be the silent bug).
  #beginTurn(): void {
    if (this.#turnInFlight) throw new Error("a turn is already in flight on this session (one turn at a time)");
    this.#turnInFlight = true; this.#done = false; this.#failed = null; this.#msg = "";
  }

  async run(input: string, timeoutMs = 180_000): Promise<{ ok: boolean; failed: string | null; message: string }> {
    this.#beginTurn();
    const ack = await this.#send("turn/start", { threadId: this.#threadId, input: [{ type: "text", text: input, text_elements: [] }] }) as Record<string, unknown>;
    if (ack?.["__error"] !== undefined) { this.#turnInFlight = false; throw new Error(`turn/start rejected: ${JSON.stringify(ack["__error"])}`); }
    const deadline = Date.now() + timeoutMs;
    while (!this.#done) { if (Date.now() > deadline) { this.#turnInFlight = false; throw new Error(`turn timeout after ${timeoutMs}ms`); } await new Promise((r) => setTimeout(r, 300)); }
    return { ok: this.#done && !this.#failed, failed: this.#failed, message: this.#msg };
  }

  setGoal(objective: string, tokenBudget?: number): Promise<unknown> { return this.#send("thread/goal/set", { threadId: this.#threadId, objective, ...(tokenBudget != null ? { tokenBudget } : {}) }); }
  getGoal(): Promise<unknown> { return this.#send("thread/goal/get", { threadId: this.#threadId }); }
  fork(): Promise<unknown> { return this.#send("thread/fork", { threadId: this.#threadId }); }
  steer(input: string): Promise<unknown> { return this.#send("turn/steer", { threadId: this.#threadId, expectedTurnId: this.#activeTurn, input: [{ type: "text", text: input, text_elements: [] }] }); }
  interrupt(): Promise<unknown> { return this.#send("turn/interrupt", { threadId: this.#threadId, turnId: this.#activeTurn }); }
  /** Fire a turn without awaiting completion (for steer/interrupt scenarios). */
  startTurnAsync(input: string): void { this.#beginTurn(); this.#send("turn/start", { threadId: this.#threadId, input: [{ type: "text", text: input, text_elements: [] }] }).catch(() => { this.#turnInFlight = false; }); }
  async awaitTurn(timeoutMs = 120_000): Promise<{ ok: boolean; message: string }> { const d = Date.now() + timeoutMs; while (!this.#done && Date.now() < d) await new Promise((r) => setTimeout(r, 300)); return { ok: this.#done && !this.#failed, message: this.#msg }; }

  close(): void { this.#die("session closed"); this.#proc.kill("SIGKILL"); }
}
