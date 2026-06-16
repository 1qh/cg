.PHONY: bridge-bootstrap bridge-run rs-lint codex-drift catalog catalog-check help

## bridge-bootstrap: build the pure-Rust bridge binary (cargo release)
bridge-bootstrap:
	@scripts/bridge.sh bootstrap

## bridge-run: launch the pure-Rust responses bridge (PORT=4011)
bridge-run:
	@scripts/bridge.sh run

## rs-lint: max-strict lintmax-rs gate over the bridge crate
rs-lint:
	@cd bridge && cargo lintmax check >/dev/null 2>&1 && echo ok || cargo lintmax check

## codex-drift: assert the bridge's faithful codex-type mirror still covers codex source (CI drift gate)
codex-drift:
	@scripts/codex-mirror-drift-check.sh

## catalog: regenerate the model catalog JSON from the typed source (gen-catalog)
catalog:
	@cd bridge && cargo build --release --bin gen-catalog -q && ./target/release/gen-catalog

## catalog-check: fail if the committed catalog is stale vs the typed source (codegen freshness gate)
catalog-check:
	@cd bridge && cargo build --release --bin gen-catalog -q && cp gemini-catalog.json /tmp/catalog.bak && ./target/release/gen-catalog >/dev/null && if diff -q /tmp/catalog.bak gemini-catalog.json >/dev/null; then echo ok; else cp /tmp/catalog.bak gemini-catalog.json; echo "catalog stale: run make catalog" >&2; exit 1; fi

## help: list targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/## //'
