# codex-byok

BYOK substrate: drive the OpenAI Codex engine on a bring-your-own-key Gemini model, as if it were a native
OpenAI responses model. Open-by-default substrate; products are a thin private delta on top.

- Architecture, decisions, and runbooks: the sibling `codex-byok-doc` repo (`adr/`, `runbooks/`, `STACK.md`).
- Stack: `@openai/codex-sdk` / `codex app-server` → codex engine → patched LiteLLM `/v1/responses` proxy → Gemini.
- Run the proxy as a subprocess: `scripts/bridge-subprocess.sh bootstrap` then `... run`.
