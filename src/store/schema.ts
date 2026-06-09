// Typed persistence schema for threads + turns. Declared in TS; drizzle-kit generates the migrations.
// Closed-set values (status) are typed unions enforced at the boundary, never bare strings.
import { sqliteTable, text, integer } from "drizzle-orm/sqlite-core";

export type ThreadStatus = "active" | "compacted" | "archived";
export type TurnStatus = "completed" | "failed" | "aborted";

export const threads = sqliteTable("threads", {
  id: text("id").primaryKey(),
  title: text("title").notNull(),
  model: text("model").notNull(),
  status: text("status").$type<ThreadStatus>().notNull().default("active"),
  /** id of the thread this one was seeded from on an app-layer compaction reset, if any. */
  seededFrom: text("seeded_from"),
  createdAt: integer("created_at").notNull(),
  updatedAt: integer("updated_at").notNull(),
});

export const turns = sqliteTable("turns", {
  id: text("id").primaryKey(),
  threadId: text("thread_id").notNull().references(() => threads.id),
  input: text("input").notNull(),
  finalResponse: text("final_response").notNull().default(""),
  status: text("status").$type<TurnStatus>().notNull(),
  /** JSON-encoded usage payload from the engine. */
  usageJson: text("usage_json"),
  createdAt: integer("created_at").notNull(),
});

export type Thread = typeof threads.$inferSelect;
export type NewThread = typeof threads.$inferInsert;
export type Turn = typeof turns.$inferSelect;
export type NewTurn = typeof turns.$inferInsert;
