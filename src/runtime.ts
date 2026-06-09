// Typed thread façade over the codex engine, driven against a BYOK model through the responses bridge.
// Wires the provider entirely in code; fails fast on missing config (no silent fallback). The proven
// Gemini-safe defaults live here so product code never re-derives them.
import { Codex } from "@openai/codex-sdk";
import type { ThreadItem, ThreadEvent, ThreadOptions } from "@openai/codex-sdk";
import { runResilient, type ResilienceOptions } from "./resilience.ts";

/** Bridge connection. Every field required — a missing value throws at construction, never defaults. */
export interface BridgeConfig {
  /** Base URL of the patched LiteLLM responses bridge, e.g. http://localhost:4011/v1 */
  readonly baseUrl: string;
  /** Master key the bridge expects. */
  readonly apiKey: string;
  /** BYOK model id registered in the catalog, e.g. gemini-3.5-flash */
  readonly model: string;
}

export type SandboxMode = "read-only" | "workspace-write" | "danger-full-access";
export type ApprovalPolicy = "never" | "on-request" | "on-failure" | "untrusted";

export interface SessionOptions {
  readonly workingDirectory: string;
  readonly sandboxMode?: SandboxMode;
  readonly approvalPolicy?: ApprovalPolicy;
  readonly skipGitRepoCheck?: boolean;
}

export interface TurnResult {
  readonly items: readonly ThreadItem[];
  readonly finalResponse: string;
  readonly usage: unknown;
  readonly threadId: string | null;
}

function requireField(cfg: BridgeConfig): void {
  for (const k of ["baseUrl", "apiKey", "model"] as const) {
    if (!cfg[k]) throw new Error(`BridgeConfig.${k} is required (no fallback); refusing to construct runtime`);
  }
}

export class CodexRuntime {
  readonly #codex: Codex;
  readonly #model: string;

  constructor(cfg: BridgeConfig) {
    requireField(cfg);
    this.#model = cfg.model;
    this.#codex = new Codex({
      config: {
        model_provider: "litellm",
        model_providers: {
          litellm: { name: "BYOK via LiteLLM", base_url: cfg.baseUrl, wire_api: "responses", env_key: "LITELLM_KEY" },
        },
      },
      env: { ...process.env, LITELLM_KEY: cfg.apiKey } as Record<string, string>,
    });
  }

  /** Open a thread with the proven BYOK-safe defaults; caller overrides only what it must. */
  startSession(opts: SessionOptions) {
    const threadOpts: ThreadOptions = {
      model: this.#model,
      // Native OS sandbox (macOS Seatbelt) — danger-full-access is a Docker-only workaround, never here.
      sandboxMode: opts.sandboxMode ?? "workspace-write",
      approvalPolicy: opts.approvalPolicy ?? "on-request",
      skipGitRepoCheck: opts.skipGitRepoCheck ?? true,
      // Gemini rejects the built-in web_search tool mixed with function tools; grounding is the bridge's job.
      webSearchMode: "disabled",
      workingDirectory: opts.workingDirectory,
    };
    const thread = this.#codex.startThread(threadOpts);
    return {
      get id(): string | null { return thread.id ?? null; },
      async run(input: string): Promise<TurnResult> {
        const turn = await thread.run(input);
        return { items: turn.items, finalResponse: turn.finalResponse, usage: turn.usage, threadId: thread.id ?? null };
      },
      // Turn execution under the resilience policy: a turn that throws (throttle / transport failure) is
      // retried with deadline + exponential backoff. This is the product path — turn-completion degrades
      // under burst, so the agent retries rather than surfacing a failure.
      async runResilient(input: string, opts?: ResilienceOptions): Promise<TurnResult> {
        return runResilient(async () => {
          const turn = await thread.run(input);
          return { items: turn.items, finalResponse: turn.finalResponse, usage: turn.usage, threadId: thread.id ?? null };
        }, opts);
      },
      async *stream(input: string): AsyncGenerator<ThreadEvent> {
        const { events } = await thread.runStreamed(input);
        for await (const ev of events as AsyncGenerator<ThreadEvent>) yield ev;
      },
    };
  }
}
