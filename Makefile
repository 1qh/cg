.PHONY: bridge-bootstrap bridge-run verify verify-live harness parity-tiers help

## bridge-bootstrap: build the pure-Rust bridge binary (cargo release)
bridge-bootstrap:
	@scripts/bridge.sh bootstrap

## bridge-run: launch the pure-Rust responses bridge (PORT=4011)
bridge-run:
	@scripts/bridge.sh run

## verify: offline can-fail suite (no API spend) — bridge self-check + primitives
verify:
	@node --test verify/resilience.mjs verify/secret-store.mjs verify/store.mjs >/dev/null 2>&1 && echo ok || node --test verify/resilience.mjs verify/secret-store.mjs verify/store.mjs

## verify-live: live capability suite on the real Gemini path (needs GEMINI_API_KEY)
verify-live:
	@node --test verify/harness-live.mjs verify/harness-advanced-live.mjs verify/harness-final-live.mjs verify/app-server-session-live.mjs verify/burst-recovery-live.mjs verify/burst-sustained-live.mjs verify/mcp-live.mjs verify/image-input-live.mjs verify/code-execution-live.mjs verify/parity-live.mjs verify/runtime-live.mjs verify/compaction-live.mjs

## harness: comprehensive harness capability suite on the real path (PARITY_MODEL overrides tier)
harness:
	@node --test verify/harness-live.mjs

## parity-tiers: assert capability parity across every model tier (pro/flash/flash-lite)
parity-tiers:
	@for m in gemini-3.1-pro-preview gemini-3.5-flash gemini-3.1-flash-lite; do \
		PARITY_MODEL=$$m node --test verify/harness-live.mjs verify/parity-live.mjs verify/code-execution-live.mjs verify/image-input-live.mjs >/dev/null 2>&1 || { echo "tier $$m FAILED" >&2; exit 1; }; \
	done; echo ok

## help: list targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/## //'

## codex-drift: assert the bridge's faithful codex-type mirror still covers codex source (CI drift gate)
codex-drift:
	@scripts/codex-mirror-drift-check.sh
