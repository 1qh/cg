## bridge-bootstrap: create the proxy venv + install the pinned patched litellm
bridge-bootstrap:
	@scripts/bridge.sh bootstrap

## bridge-run: launch the patched responses bridge (STRICT self-check; PORT=4011)
bridge-run:
	@scripts/bridge.sh run

## parity-live: live capability parity (multi-step coding + grounding) on the real Gemini path
parity-live:
	@node --test verify/parity-live.mjs

## verify: run the substrate can-fail verification suite
verify:
	@node --test verify/ >/dev/null 2>&1 && echo ok || node --test verify/

## help: list targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/## //'
