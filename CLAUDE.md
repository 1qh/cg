# CLAUDE.md

Rust adapter repo for the BYOK codex-on-Gemini substrate: the pure-Rust `/v1/responses` bridge binary. The TS
client-core libs live in the sibling `cg-ts` repo; prose lives in `cg-doc` and `book`. This repo carries
machine-readable config + this pointer + `README.md` only.

- Rules: `.claude/rules/book` (generic) + `.claude/rules/project-doc` (this project's ADRs/runbooks) auto-load
  and re-inject after compaction.
- Session start: read every `book` doc, then every `cg-doc` ADR + runbook, then this repo's layout, before any
  task — per `book/CLAUDE.md` and `cg-doc/adr/foundation-bootstrap-order.md`.
- Single-language rust repo: the whole gate is `make rs-lint` (lintmax-rs) over the bridge crate.
- The sibling `cg-ts` live verify consumes this repo's built bridge binary via its `CG_DIR` (default: the sibling `cg` repo).
- Operator-local paths, ports, secret locations, and the GitHub org live in `CLAUDE.local.md` (gitignored).
