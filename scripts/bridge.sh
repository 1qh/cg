#!/usr/bin/env bash
# Run the codex-on-Gemini pure-Rust /v1/responses bridge as a plain SUBPROCESS (no Docker, no Python).
#   bootstrap:  scripts/bridge.sh bootstrap        (cargo build --release of the bridge crate)
#   run:        scripts/bridge.sh run [PORT]        (launches the bridge binary)
set -euo pipefail
D="$(cd "$(dirname "${0}")/.." && pwd)"
BIN="${D}/bridge/target/release/codex-gemini-bridge"
PORT="${2:-4011}"
case "${1:-run}" in
  bootstrap)
    cargo build --release --quiet --manifest-path "${D}/bridge/Cargo.toml"
    echo ok
    ;;
  run)
    : "${GEMINI_API_KEY:?set GEMINI_API_KEY}"
    [[ ${INJECT_GROUNDING:-} == "1" ]] && export GROUNDING=1 || true
    exec env PORT="${PORT}" "${BIN}"
    ;;
  *)
    echo "usage: ${0} {bootstrap|run [PORT]}" >&2
    exit 2
    ;;
esac
