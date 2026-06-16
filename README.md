# cg

Rust adapter for the BYOK codex-on-Gemini substrate: a pure-Rust `/v1/responses` bridge that drives the
OpenAI Codex engine on a bring-your-own-key Gemini model, as if it were a native OpenAI responses model.
Open-by-default substrate; products are a thin private delta on top.

- Architecture, decisions, and runbooks: the sibling `cg-doc` repo (`adr/`, `runbooks/`, `STACK.md`).
- TS client-core libs (resilience, store, keychain, façades, verify harness): the sibling `cg-ts` repo.
- Stack: `@openai/codex-sdk` / `codex app-server` → codex engine → this pure-Rust axum `/v1/responses` bridge
  (vendored gemini-rust + async-openai event types) → Gemini.
- Build + run the bridge: `make bridge-bootstrap` then `make bridge-run` (PORT=4011).
- Gate: `make rs-lint` (lintmax-rs) · `make codex-drift` (mirror drift) · `make catalog-check` (codegen freshness).
