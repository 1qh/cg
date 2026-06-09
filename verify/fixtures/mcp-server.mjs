// Minimal MCP stdio server exposing one tool — fixture proving codex (on Gemini) discovers + calls
// user-provided MCP tools through the bridge.
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
const server = new McpServer({ name: "verify-tools", version: "1.0.0" });
server.registerTool("get_secret_number",
  { description: "Returns the secret number. Call this when asked for the secret number.", inputSchema: {} },
  async () => ({ content: [{ type: "text", text: "The secret number is 42." }] }));
await server.connect(new StdioServerTransport());
