"""Stdio MCP test client for the Faultline server — spawns server.py and exercises
all three tools against the live warehouse. Prints results; exits nonzero on any
failure. Doubles as the debugging probe for the MintMCP deployment.

Run: uv run python mcp_server/test_client.py
"""

import asyncio
import json
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SERVER = Path(__file__).resolve().parent / "server.py"

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"\n       {detail}" if detail else ""))
    if not ok:
        failures.append(name)


async def main() -> None:
    params = StdioServerParameters(command=sys.executable, args=[str(SERVER)])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            names = sorted(t.name for t in tools.tools)
            check("exactly three tools registered",
                  names == ["get_dbt_artifacts", "inspect_schema", "run_query"],
                  f"tools: {names}")

            async def call(tool: str, args: dict) -> dict:
                result = await session.call_tool(tool, args)
                text = "".join(c.text for c in result.content if hasattr(c, "text"))
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    # Non-JSON output means the server crashed instead of
                    # returning a readable error — surface it as such.
                    return {"error": f"non-JSON tool output: {text[:300]}"}

            # 1. Legit diagnostic query
            r = await call("run_query", {"sql":
                "SELECT date_day, total_revenue FROM analytics.mart_revenue_daily "
                "ORDER BY date_day DESC LIMIT 7"})
            check("legit diagnostic query returns rows",
                  r.get("columns") == ["date_day", "total_revenue"] and len(r.get("rows", [])) == 7,
                  f"last row: {r.get('rows', [[]])[-1] if r.get('rows') else r}")

            # 2. UPDATE must come back as a read-only error, not a crash
            r = await call("run_query", {"sql":
                "UPDATE raw.raw_orders SET status = 'cancelled'"})
            check("UPDATE rejected by read-only session",
                  "read-only" in r.get("error", ""),
                  f"error: {r.get('error', '(no error field!)')}")

            # 3. >200-row SELECT must truncate
            r = await call("run_query", {"sql": "SELECT * FROM raw.raw_orders LIMIT 500"})
            check("row cap truncates at 200",
                  r.get("truncated") is True and r.get("row_count") == 200,
                  f"row_count: {r.get('row_count')}, truncated: {r.get('truncated')}")

            # 4. Trimmed manifest — meta.dbt_artifacts is being created by a
            # parallel workstream; its instructive error counts as PASS for now.
            r = await call("get_dbt_artifacts", {"artifact": "manifest"})
            if "error" in r:
                check("get_dbt_artifacts('manifest') — table pending, instructive error",
                      "meta.dbt_artifacts" in r["error"], f"error: {r['error']}")
            else:
                size_kb = len(json.dumps(r)) / 1024
                check("get_dbt_artifacts('manifest') — trimmed manifest",
                      "nodes" in r and "sources" in r and len(r["nodes"]) > 0,
                      f"{len(r.get('nodes', {}))} nodes, {len(r.get('sources', {}))} sources, "
                      f"trimmed size {size_kb:.1f} KB")

            # 5. inspect_schema with a bare table name (should resolve to raw)
            r = await call("inspect_schema", {"table": "raw_orders"})
            check("inspect_schema('raw_orders') resolves and samples",
                  r.get("schema") == "raw" and r.get("row_count", 0) > 0
                  and len(r.get("sample_rows", {}).get("rows", [])) == 5,
                  f"schema: {r.get('schema')}, row_count: {r.get('row_count')}, "
                  f"columns: {[c['name'] for c in r.get('columns', [])]}")

    print()
    if failures:
        print(f"{len(failures)} FAILED: {failures}")
        sys.exit(1)
    print("ALL PASS")


if __name__ == "__main__":
    asyncio.run(main())
