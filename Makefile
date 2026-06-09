.PHONY: bridge-bootstrap bridge-run verify verify-live harness help

## bridge-bootstrap: create the proxy venv + install the pinned patched litellm
bridge-bootstrap:
	@scripts/bridge.sh bootstrap

## bridge-run: launch the patched responses bridge (STRICT self-check; PORT=4011)
bridge-run:
	@scripts/bridge.sh run

## verify: offline can-fail suite (no API spend) — bridge self-check + primitives
verify:
	@node --test verify/bridge-selfcheck.mjs verify/resilience.mjs verify/secret-store.mjs verify/store.mjs >/dev/null 2>&1 && echo ok || node --test verify/bridge-selfcheck.mjs verify/resilience.mjs verify/secret-store.mjs verify/store.mjs

## verify-live: live capability suite on the real Gemini path (needs GEMINI_API_KEY)
verify-live:
	@node --test verify/harness-live.mjs verify/parity-live.mjs verify/runtime-live.mjs verify/compaction-live.mjs

## harness: comprehensive harness capability suite on the real path (PARITY_MODEL overrides tier)
harness:
	@node --test verify/harness-live.mjs

## help: list targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/## //'
