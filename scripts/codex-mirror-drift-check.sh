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
base="https://raw.githubusercontent.com/openai/codex/${tag}/codex-rs/protocol/src"
efforts_src="$(curl -fsSL "${base}/openai_models.rs")"
models_src="$(curl -fsSL "${base}/models.rs")"
bridge="$(cat "${root}/bridge/src/main.rs")"

missing=""
# (1) every ReasoningEffort wire value codex defines must be handled by CodexEffort.
efforts="$(grep -oE '"(none|minimal|low|medium|high|xhigh)"' <<< "${efforts_src}" | tr -d '"' | sort -u)"
for e in ${efforts}; do
  grep -qF "\"${e}\"" <<< "${bridge}" || missing="${missing} effort:${e}"
done
# (2) every ResponseItem variant the bridge EXPLICITLY handles must still exist in codex's enum;
# a rename would silently route it to the catch-all and break the agentic loop (e.g. lost tool output).
response_item="$(sed -n '/pub enum ResponseItem/,/^}/p' <<< "${models_src}")"
for v in Message Reasoning FunctionCall FunctionCallOutput; do
  grep -qE "^[[:space:]]+${v}[[:space:]]*\{" <<< "${response_item}" || missing="${missing} input:${v}"
done
# (2b) the EMIT side: the bridge serializes its output items (via async-openai OutputItem) to these
# ResponseItem fields; codex SILENTLY DROPS an item whose shape drifts, so a renamed field is a quiet
# content loss the harness may not catch. Assert each emit-populated field still exists in ResponseItem.
for f in role content summary encrypted_content name call_id arguments; do
  grep -qE "^[[:space:]]+${f}:" <<< "${response_item}" || missing="${missing} emit-field:${f}"
done
# (3) every ContentItem variant the bridge mirrors must still exist with its load-bearing field;
# a renamed variant/field silently drops text or images (the class of the image-input regression).
content_item="$(sed -n '/pub enum ContentItem/,/^}/p' <<< "${models_src}")"
for v in InputText InputImage OutputText; do
  grep -qE "^[[:space:]]+${v}[[:space:]]*\{" <<< "${content_item}" || missing="${missing} content:${v}"
done
grep -qE "^[[:space:]]+image_url:" <<< "${content_item}" || missing="${missing} content-field:image_url"
grep -qE "^[[:space:]]+text:" <<< "${content_item}" || missing="${missing} content-field:text"
# (4) every ImageDetail value the bridge maps to a gemini media-resolution must still exist;
# a new value an unmapped mirror would route to Unknown (model default) instead of the caller's intent.
image_detail="$(sed -n '/pub enum ImageDetail/,/^}/p' <<< "${models_src}")"
for v in Auto High Low Original; do
  grep -qE "^[[:space:]]+${v}," <<< "${image_detail}" || missing="${missing} detail:${v}"
done
# (5) function_call_output.output is codex's untagged FunctionCallOutputBody (string OR content items);
# the bridge mirrors both forms — a renamed variant would silently fail to parse the array form (422).
output_body="$(sed -n '/pub enum FunctionCallOutputBody/,/^}/p' <<< "${models_src}")"
for v in Text ContentItems; do
  grep -qE "^[[:space:]]+${v}" <<< "${output_body}" || missing="${missing} outputbody:${v}"
done

if [[ -n ${missing} ]]; then
  printf 'DRIFT: codex types the bridge mirror no longer matches:%s — update the mirror\n' "${missing}" >&2
  exit 1
fi
echo ok
