#!/usr/bin/env bash
# Run the codex-on-Gemini LiteLLM bridge as a plain SUBPROCESS (no Docker).
# Proves/enables the lightweight desktop-app deployment: pip install + patch dir + run.
#   bootstrap:  scripts/bridge-subprocess.sh bootstrap   (creates venv, installs litellm[proxy]==1.88.1)
#   run:        scripts/bridge-subprocess.sh run [PORT]   (launches the patched proxy, STRICT self-check)
set -euo pipefail
D="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$D/.litellm-venv"; PORT="${2:-4011}"
case "${1:-run}" in
  bootstrap)
    PYBIN="$(command -v python3.13 || command -v python3.12 || command -v python3.11)"
    "$PYBIN" -m venv "$VENV"
    "$VENV/bin/pip" install -q "litellm[proxy]==1.88.1"
    echo ok ;;
  run)
    : "${GEMINI_API_KEY:?set GEMINI_API_KEY}"
    export PYTHONPATH="$D/litellm_patch" LITELLM_MASTER_KEY="${LITELLM_MASTER_KEY:-sk-spike-local}" LITELLM_PATCH_STRICT=1
    [ "${LITELLM_INJECT_GROUNDING:-}" = "1" ] && export LITELLM_INJECT_GROUNDING=1 || true
    exec "$VENV/bin/litellm" --config "$D/bridge/litellm-config.yaml" --port "$PORT" ;;
  *) echo "usage: $0 {bootstrap|run [PORT]}" >&2; exit 2 ;;
esac
