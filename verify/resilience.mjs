// Can-fail proof of the turn-resilience policy. Pure — zero API spend.
//  GREEN-retry: a unit that fails twice then succeeds is driven to success by the retry policy.
//  RED-deadline: a unit that hangs past the per-attempt deadline is aborted (timeout fires, attempts exhaust).
//  RED-exhaust: a unit that always fails exhausts maxAttempts and throws (no infinite retry).
import { test } from "node:test";
import assert from "node:assert/strict";
import { runResilient } from "../src/resilience.ts";

const FAST = { attemptTimeoutMs: 300, maxAttempts: 3, initialBackoffMs: 10, maxBackoffMs: 40 };

test("GREEN: fails twice then succeeds -> policy retries to success", async () => {
  let calls = 0;
  const out = await runResilient(async () => {
    calls++;
    if (calls < 3) throw new Error("transient non-completion");
    return "done";
  }, FAST);
  assert.equal(out, "done");
  assert.equal(calls, 3, "must have taken exactly 3 attempts");
});

test("RED: a hanging attempt is deadline-aborted, then attempts exhaust", async () => {
  let aborts = 0;
  await assert.rejects(
    runResilient((signal) => new Promise((_, reject) => {
      signal.addEventListener("abort", () => { aborts++; reject(new Error("aborted")); });
    }), FAST),
  );
  assert.ok(aborts >= 1, "the per-attempt deadline must have aborted at least one hanging attempt");
});

test("RED: always-fails exhausts maxAttempts and throws (no infinite retry)", async () => {
  let calls = 0;
  await assert.rejects(runResilient(async () => { calls++; throw new Error("permanent"); }, FAST));
  assert.equal(calls, 3, "must stop at maxAttempts, not retry forever");
});
