#!/usr/bin/env bash
# The bridge mirrors codex's request types (it must not depend on codex-protocol — that drags a
# vulnerable network tree). This check keeps the mirror FAITHFUL: it fetches codex's ReasoningEffort
# at the pinned tag and asserts the bridge handles every effort value codex defines. A new codex
# value fails this, forcing a mirror update — the SSOT is codex source, the mirror the verified copy.
# Agent-first: prints `ok` + exits 0 on success; verbose only on drift.
set -euo pipefail
here="$(dirname "${0}")"
root="$(cd "${here}/.." && pwd)"
tag="$(cat "${root}/bridge/.codex-pin")"
src="$(curl -fsSL "https://raw.githubusercontent.com/openai/codex/${tag}/codex-rs/protocol/src/openai_models.rs")"
bridge="$(cat "${root}/bridge/src/main.rs")"
# codex's ReasoningEffort wire values (the string arms in its FromStr/Display).
efforts="$(grep -oE '"(none|minimal|low|medium|high|xhigh)"' <<< "${src}" | tr -d '"' | sort -u)"
missing=""
for e in ${efforts}; do
  grep -qF "\"${e}\"" <<< "${bridge}" || missing="${missing} ${e}"
done
if [[ -n ${missing} ]]; then
  printf 'DRIFT: codex efforts not handled by the bridge mirror:%s — update CodexEffort\n' "${missing}" >&2
  exit 1
fi
echo ok
