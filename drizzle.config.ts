import { defineConfig } from "drizzle-kit";

// Generates the thread/turn migrations from the typed schema (SSOT). The store applies these via the
// drizzle migrator — never a hand-written DDL twin (typed-domain.md: codegen owns the schema, zero hand-SQL).
export default defineConfig({
  dialect: "sqlite",
  schema: "./src/store/schema.ts",
  out: "./src/store/migrations",
});
