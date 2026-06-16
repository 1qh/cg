.PHONY: bridge-bootstrap bridge-run verify verify-live harness parity-tiers catalog catalog-check migrations-check help

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

## catalog: regenerate the model catalog JSON from the typed source (gen-catalog)
catalog:
	@cd bridge && cargo build --release --bin gen-catalog -q && ./target/release/gen-catalog

## catalog-check: fail if the committed catalog is stale vs the typed source (codegen freshness gate)
catalog-check:
	@cd bridge && cargo build --release --bin gen-catalog -q && cp gemini-catalog.json /tmp/catalog.bak && ./target/release/gen-catalog >/dev/null && if diff -q /tmp/catalog.bak gemini-catalog.json >/dev/null; then echo ok; else cp /tmp/catalog.bak gemini-catalog.json; echo "catalog stale: run make catalog" >&2; exit 1; fi

## migrations-check: fail if the store schema changed without a regenerated drizzle migration (codegen freshness)
migrations-check:
	@n0=$$(ls src/store/migrations/*.sql 2>/dev/null | wc -l | tr -d " "); node_modules/.bin/drizzle-kit generate >/dev/null 2>&1; n1=$$(ls src/store/migrations/*.sql 2>/dev/null | wc -l | tr -d " "); if [ "$$n0" = "$$n1" ]; then echo ok; else echo "store schema.ts changed without a regenerated migration: run drizzle-kit generate + commit" >&2; exit 1; fi
