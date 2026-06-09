# CLAUDE.md

Code repo for the BYOK codex-on-Gemini substrate. Prose lives in the sibling `codex-byok-doc` repo and in
`book`; this repo carries machine-readable config + this pointer + `README.md` only.

- Rules: `.claude/rules/book` (generic) + `.claude/rules/project-doc` (this project's ADRs/runbooks) auto-load
  and re-inject after compaction.
- Session start: read every `book` doc, then every `codex-byok-doc` ADR + runbook, then this repo's layout,
  before any task — per `book/CLAUDE.md` and `codex-byok-doc/adr/foundation-bootstrap-order.md`.
- Operator-local paths, ports, secret locations, and the GitHub org live in `CLAUDE.local.md` (gitignored).
