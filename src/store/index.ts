// Typed thread/turn persistence over better-sqlite3 via Drizzle. The schema is created from the declared
// tables; product code never writes raw SQL. Persists across app restart so conversations reopen.
import Database from "better-sqlite3";
import { drizzle, type BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import { eq } from "drizzle-orm";
import { threads, turns, type NewThread, type NewTurn, type Thread, type Turn } from "./schema.ts";

// DDL mirrors schema.ts; drizzle-kit owns it as a generated migration in production. Inline here so a fresh
// store (and the verify harness) is self-standing without a migration runner.
const DDL = `
CREATE TABLE IF NOT EXISTS threads (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, model TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active', seeded_from TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS turns (
  id TEXT PRIMARY KEY, thread_id TEXT NOT NULL REFERENCES threads(id),
  input TEXT NOT NULL, final_response TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL, usage_json TEXT, created_at INTEGER NOT NULL);
`;

export class ThreadStore {
  readonly #db: BetterSQLite3Database;
  readonly #raw: Database.Database;

  constructor(path: string) {
    if (!path) throw new Error("ThreadStore requires a db path (use ':memory:' for ephemeral)");
    this.#raw = new Database(path);
    this.#raw.pragma("journal_mode = WAL");
    this.#raw.pragma("foreign_keys = ON");
    this.#raw.exec(DDL);
    this.#db = drizzle(this.#raw);
  }

  createThread(t: NewThread): void { this.#db.insert(threads).values(t).run(); }
  getThread(id: string): Thread | undefined { return this.#db.select().from(threads).where(eq(threads.id, id)).get(); }
  listThreads(): Thread[] { return this.#db.select().from(threads).all(); }
  setThreadStatus(id: string, status: Thread["status"], updatedAt: number): void {
    this.#db.update(threads).set({ status, updatedAt }).where(eq(threads.id, id)).run();
  }

  addTurn(t: NewTurn): void { this.#db.insert(turns).values(t).run(); }
  turnsFor(threadId: string): Turn[] { return this.#db.select().from(turns).where(eq(turns.threadId, threadId)).all(); }

  close(): void { this.#raw.close(); }
}
