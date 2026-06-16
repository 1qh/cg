// Typed thread/turn persistence over better-sqlite3 via Drizzle. The schema is created by APPLYING the
// drizzle-kit-generated migrations (SSOT = schema.ts); product code never writes raw/hand-written SQL.
// Persists across app restart so conversations reopen.
import Database from "better-sqlite3";
import { drizzle, type BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { eq, asc } from "drizzle-orm";
import { fileURLToPath } from "node:url";
import { threads, turns, type NewThread, type NewTurn, type Thread, type Turn } from "./schema.ts";

const MIGRATIONS = fileURLToPath(new URL("./migrations", import.meta.url));

export class ThreadStore {
  readonly #db: BetterSQLite3Database;
  readonly #raw: Database.Database;

  constructor(path: string) {
    if (!path) throw new Error("ThreadStore requires a db path (use ':memory:' for ephemeral)");
    this.#raw = new Database(path);
    this.#raw.pragma("journal_mode = WAL");
    this.#raw.pragma("foreign_keys = ON");
    this.#db = drizzle(this.#raw);
    migrate(this.#db, { migrationsFolder: MIGRATIONS });
  }

  createThread(t: NewThread): void { this.#db.insert(threads).values(t).run(); }
  getThread(id: string): Thread | undefined { return this.#db.select().from(threads).where(eq(threads.id, id)).get(); }
  listThreads(): Thread[] { return this.#db.select().from(threads).all(); }
  setThreadStatus(id: string, status: Thread["status"], updatedAt: number): void {
    this.#db.update(threads).set({ status, updatedAt }).where(eq(threads.id, id)).run();
  }

  addTurn(t: NewTurn): void { this.#db.insert(turns).values(t).run(); }
  turnsFor(threadId: string): Turn[] { return this.#db.select().from(turns).where(eq(turns.threadId, threadId)).orderBy(asc(turns.createdAt), asc(turns.id)).all(); }

  /** Run a compound write atomically (e.g. create a thread + its first turn) — all-or-nothing. */
  transaction<T>(fn: () => T): T { return this.#raw.transaction(fn)(); }

  close(): void { this.#raw.close(); }
}
