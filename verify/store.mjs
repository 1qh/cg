// Can-fail proof of thread/turn persistence. Includes a real reopen to prove survival across app restart.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ThreadStore } from "../src/store/index.ts";

const T0 = 1_700_000_000_000; // fixed stamp — Date.now() banned in this codebase's deterministic tests

test("persists threads + turns and reads them back typed", () => {
  const store = new ThreadStore(":memory:");
  store.createThread({ id: "th1", title: "first", model: "gemini-3.5-flash", createdAt: T0, updatedAt: T0 });
  store.addTurn({ id: "tn1", threadId: "th1", input: "hello", finalResponse: "hi", status: "completed", createdAt: T0 });
  const th = store.getThread("th1");
  assert.equal(th?.status, "active", "default status applied");
  assert.equal(store.turnsFor("th1").length, 1);
  assert.equal(store.turnsFor("th1")[0].status, "completed");
  store.close();
});

test("survives a reopen (persistence across app restart)", () => {
  const dir = mkdtempSync(join(tmpdir(), "store-"));
  const path = join(dir, "threads.db");
  const a = new ThreadStore(path);
  a.createThread({ id: "keep", title: "persisted", model: "gemini-3.5-flash", createdAt: T0, updatedAt: T0 });
  a.setThreadStatus("keep", "compacted", T0 + 1);
  a.close();

  const b = new ThreadStore(path); // simulates restart
  const th = b.getThread("keep");
  assert.equal(th?.title, "persisted", "thread must survive the reopen");
  assert.equal(th?.status, "compacted", "status update must have persisted");
  b.close();
});

test("rejects an empty db path (fail-fast, no implicit default)", () => {
  assert.throws(() => new ThreadStore(""), /requires a db path/);
});
