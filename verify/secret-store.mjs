// Can-fail proof of OS-keychain secret storage + redaction. Uses a throwaway account, cleans up. No API spend.
import { test } from "node:test";
import assert from "node:assert/strict";
import { SecretStore, redact } from "../src/secret-store.ts";

test("round-trips a secret through the OS keychain and deletes it", () => {
  const store = new SecretStore("codex-byok-verify");
  const account = "verify-byok-key";
  const secret = "sk-test-" + "x".repeat(32);
  try {
    store.set(account, secret);
    assert.equal(store.get(account), secret, "stored secret must read back identical");
    assert.equal(store.delete(account), true, "delete must report success");
    assert.equal(store.get(account), null, "deleted secret must read back null, never a fallback");
  } finally {
    store.delete(account);
  }
});

test("refuses to store an empty secret (fail-fast, no silent no-op)", () => {
  const store = new SecretStore("codex-byok-verify");
  assert.throws(() => store.set("x", ""), /empty secret/);
  assert.throws(() => store.set("x", "   "), /empty secret/, "whitespace-only secret must also be rejected");
});

test("redact never reveals the full secret", () => {
  const secret = "sk-abcdefghijklmnopqrstuvwxyz";
  const masked = redact(secret);
  assert.ok(!masked.includes("abcdefghij"), "redacted form must not reveal the secret's leading body");
  assert.ok(!masked.includes(secret.slice(-6)), "redacted form must not reveal the secret's trailing body");
  // bound BOTH ends: at most the 4-char prefix + 2-char suffix may appear, never a longer run
  assert.ok(!masked.includes(secret.slice(0, 6)) && !masked.includes(secret.slice(-4)), "redact must reveal at most a 4+2 fingerprint");
  assert.match(masked, /chars\)/);
});
