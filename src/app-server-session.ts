// App-server-backed session: the FULL product surface the typed SDK can't reach (goals, fork, steer,
// interrupt, resume) plus run/stream, over the codex app-server JSON-RPC. Fail-fast on missing config.
// Pairs with the SDK-based CodexRuntime (run/structured-output/resilient); this is the complete-surface path.
import { spawn, type ChildProcess } from "node:child_process";
import type { BridgeConfig, SandboxMode, ApprovalPolicy, TurnResult } from "./runtime.ts";

export interface AppServerSessionOptions {
  readonly workingDirectory: string;
  readonly sandboxMode?: SandboxMode;
  readonly approvalPolicy?: ApprovalPolicy;
  /** Path to the model-catalog JSON so codex recognizes the BYOK model. */
  readonly modelCatalogPath: string;
  readonly reasoningEffort?: string;
  readonly reasoningSummary?: string;
}

interface Pending { resolve: (v: unknown) => void; }

/** One codex app-server process + thread, exposing the complete harness surface. Call close() when done. */
export class AppServerSession {
  readonly #proc: ChildProcess;
  #id = 1;
  readonly #pending = new Map<number, Pending>();
  #buf = "";
  #threadId: string | null = null;
  #activeTurn: string | null = null;
  #done = false;
  #failed: string | null = null;
  #msg = "";
  readonly #model: string;

  constructor(cfg: BridgeConfig, opts: AppServerSessionOptions) {
    for (const k of ["baseUrl", "apiKey", "model"] as const) if (!cfg[k]) throw new Error(`BridgeConfig.${k} required`);
    if (!opts.workingDirectory || !opts.modelCatalogPath) throw new Error("workingDirectory + modelCatalogPath required");
    this.#model = cfg.model;
    const c = ["-c","model_provider=gemini","-c",'model_providers.gemini.name="g"',
      "-c",`model_providers.gemini.base_url="${cfg.baseUrl}"`,"-c",'model_providers.gemini.wire_api="responses"',
      "-c",'model_providers.gemini.env_key="BRIDGE_KEY"',"-c",`model_reasoning_effort="${opts.reasoningEffort ?? "high"}"`,
      ...(opts.reasoningSummary ? ["-c",`model_reasoning_summary="${opts.reasoningSummary}"`] : []),
      "-c",`model_catalog_json="${opts.modelCatalogPath}"`];
    this.#proc = spawn("codex", ["app-server", ...c], { env: { ...process.env, BRIDGE_KEY: cfg.apiKey }, stdio: ["pipe", "pipe", "ignore"] });
    this.#proc.stdout!.on("data", (chunk: Buffer) => this.#onData(chunk));
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
        this.#pending.get(id)!.resolve(m["result"] ?? { __error: m["error"] }); this.#pending.delete(id); continue;
      }
      if (id != null && m["method"]) { this.#proc.stdin!.write(JSON.stringify({ id, result: {} }) + "\n"); continue; }
      const method = m["method"] as string | undefined;
      const params = m["params"] as Record<string, unknown> | undefined;
      if (method === "turn/started") this.#activeTurn = (params?.["turn"] as Record<string, unknown>)?.["id"] as string ?? (params?.["turnId"] as string) ?? null;
      const item = params?.["item"] as Record<string, unknown> | undefined;
      if (item?.["type"] === "agent_message") this.#msg = (item["text"] as string) || this.#msg;
      if (method === "item/agentMessage/delta") this.#msg += (params?.["delta"] as string) || "";
      if (method === "turn/completed") this.#done = true;
      if (method === "turn/failed") { this.#failed = JSON.stringify(params).slice(0, 100); this.#done = true; }
    }
  }

  #send(method: string, params: unknown): Promise<unknown> {
    const id = this.#id++;
    this.#proc.stdin!.write(JSON.stringify({ method, id, params }) + "\n");
    return new Promise((resolve) => this.#pending.set(id, { resolve }));
  }

  async start(opts: AppServerSessionOptions): Promise<void> {
    await this.#send("initialize", { clientInfo: { name: "codex-byok", version: "0" }, capabilities: null });
    const ts = await this.#send("thread/start", { model: this.#model, modelProvider: "gemini", cwd: opts.workingDirectory, approvalPolicy: opts.approvalPolicy ?? "on-request", sandbox: opts.sandboxMode ?? "workspace-write" }) as Record<string, unknown>;
    this.#threadId = (ts?.["thread"] as Record<string, unknown>)?.["id"] as string ?? (ts?.["threadId"] as string) ?? null;
    if (!this.#threadId) throw new Error("thread/start did not return a thread id");
  }

  get threadId(): string | null { return this.#threadId; }
  get activeTurnId(): string | null { return this.#activeTurn; }

  async run(input: string, timeoutMs = 180_000): Promise<{ ok: boolean; failed: string | null; message: string }> {
    this.#done = false; this.#failed = null; this.#msg = "";
    await this.#send("turn/start", { threadId: this.#threadId, input: [{ type: "text", text: input, text_elements: [] }] });
    const deadline = Date.now() + timeoutMs;
    while (!this.#done) { if (Date.now() > deadline) throw new Error(`turn timeout after ${timeoutMs}ms`); await new Promise((r) => setTimeout(r, 300)); }
    return { ok: this.#done && !this.#failed, failed: this.#failed, message: this.#msg };
  }

  setGoal(objective: string, tokenBudget?: number): Promise<unknown> { return this.#send("thread/goal/set", { threadId: this.#threadId, objective, ...(tokenBudget != null ? { tokenBudget } : {}) }); }
  getGoal(): Promise<unknown> { return this.#send("thread/goal/get", { threadId: this.#threadId }); }
  fork(): Promise<unknown> { return this.#send("thread/fork", { threadId: this.#threadId }); }
  steer(input: string): Promise<unknown> { return this.#send("turn/steer", { threadId: this.#threadId, expectedTurnId: this.#activeTurn, input: [{ type: "text", text: input, text_elements: [] }] }); }
  interrupt(): Promise<unknown> { return this.#send("turn/interrupt", { threadId: this.#threadId, turnId: this.#activeTurn }); }
  /** Fire a turn without awaiting completion (for steer/interrupt scenarios). */
  startTurnAsync(input: string): void { this.#done = false; this.#failed = null; this.#msg = ""; void this.#send("turn/start", { threadId: this.#threadId, input: [{ type: "text", text: input, text_elements: [] }] }); }
  async awaitTurn(timeoutMs = 120_000): Promise<{ ok: boolean; message: string }> { const d = Date.now() + timeoutMs; while (!this.#done && Date.now() < d) await new Promise((r) => setTimeout(r, 300)); return { ok: this.#done && !this.#failed, message: this.#msg }; }

  close(): void { this.#proc.kill("SIGKILL"); }
}
