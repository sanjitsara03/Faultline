"""Faultline MCP server — the investigating agent's ONLY interface to the warehouse.

stdio transport, three tools (FAULTLINE_SPEC.md §2.5): run_query, get_dbt_artifacts,
inspect_schema. Deployed to MintMCP's hosted infra, so this file is self-contained:
DATABASE_URL arrives as an env var there; the .env fallback is local dev only.

All tools return a JSON string. Errors come back as {"error": "..."} tool output —
never a crash — because the agent iterates on failed SQL.
"""

import json
import os
from pathlib import Path

import psycopg2
import psycopg2.errors
from dotenv import load_dotenv
# fastmcp 2.x, not the official SDK's mcp.server.fastmcp: MintMCP's startup
# probe opens GET /mcp without the strict Accept header the official SDK
# demands (it 406s), so the probe times out and hosted deploys fail with
# empty logs. fastmcp 2.x is what MintMCP's own template documents.
from fastmcp import FastMCP
from psycopg2 import sql

ROW_CAP = 200  # hard cap on rows returned by any query

mcp = FastMCP("faultline")


def _database_url() -> str:
    # On MintMCP the env var is injected into the container; the .env fallback
    # only fires on the laptop (local dev / test_client.py).
    if not os.environ.get("DATABASE_URL"):
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set (env var, or ../.env for local dev)")
    return url


def _connect():
    # One fresh connection per tool call: boring and stateless, so the server
    # survives the Supabase pooler's idle timeouts with nothing to reconnect.
    conn = psycopg2.connect(_database_url())
    conn.autocommit = True  # each statement gets its own implicit transaction
    with conn.cursor() as cur:
        # Defense in depth: the MintMCP gateway is the governance layer being
        # demonstrated, but with a read-only session a bypassed gateway still
        # can't write. Timeout keeps a runaway agent query from hanging a call.
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET statement_timeout = '15s'")
    return conn


def _error(message: str) -> str:
    return json.dumps({"error": message})


@mcp.tool()
def run_query(sql: str) -> str:
    """Execute a SQL query against the warehouse Postgres (schemas: 'raw' sources,
    'analytics' dbt models). Returns JSON {"columns": [...], "rows": [...]}.
    Results are capped at 200 rows and flagged "truncated" when the cap is hit —
    prefer aggregates or LIMIT. The session is read-only; writes will fail.
    SQL errors return as {"error": ...} so you can revise the query."""
    try:
        conn = _connect()
    except Exception as e:
        return _error(f"could not connect to warehouse: {e}")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return json.dumps({"columns": [], "rows": [],
                                   "note": "statement produced no result set"})
            columns = [d.name for d in cur.description]
            rows = cur.fetchmany(ROW_CAP + 1)  # +1 just to detect truncation
            truncated = len(rows) > ROW_CAP
            rows = rows[:ROW_CAP]
            result = {"columns": columns, "rows": [list(r) for r in rows],
                      "row_count": len(rows)}
            if truncated:
                result["truncated"] = True
                result["note"] = (f"Result truncated to first {ROW_CAP} rows. "
                                  "Use aggregation or a tighter LIMIT/WHERE.")
            return json.dumps(result, default=str)  # default=str: Decimal/date/uuid
    except Exception as e:
        return _error(str(e).strip())
    finally:
        conn.close()


def _trim_manifest(manifest: dict) -> dict:
    # Raw manifest is multi-MB; keep only what an investigation needs so the
    # whole DAG fits in an LLM context.
    nodes = {}
    for node_id, node in manifest.get("nodes", {}).items():
        trimmed = {
            "name": node.get("name"),
            "resource_type": node.get("resource_type"),
            "depends_on": node.get("depends_on", {}).get("nodes", []),
            "description": node.get("description", ""),
            "schema": node.get("schema"),
            "materialization": node.get("config", {}).get("materialized"),
        }
        if node.get("resource_type") == "model":
            trimmed["raw_code"] = node.get("raw_code", "")
        nodes[node_id] = trimmed
    sources = {}
    for source_id, source in manifest.get("sources", {}).items():
        sources[source_id] = {
            "name": source.get("name"),
            "resource_type": "source",
            "schema": source.get("schema"),
            "relation_name": source.get("relation_name"),
            "description": source.get("description", ""),
        }
    return {"nodes": nodes, "sources": sources}


def _trim_run_results(run_results: dict) -> dict:
    return {
        "generated_at": run_results.get("metadata", {}).get("generated_at"),
        "results": [
            {"node": r.get("unique_id"), "status": r.get("status"),
             "execution_time": r.get("execution_time")}
            for r in run_results.get("results", [])
        ],
    }


@mcp.tool()
def get_dbt_artifacts(artifact: str) -> str:
    """Fetch the latest dbt artifact for the warehouse. artifact must be 'manifest'
    (the DAG: every model/source with its dependencies, SQL, schema, description,
    materialization) or 'run_results' (per-node status and execution time from the
    latest dbt run). Use the manifest to walk lineage upstream from a mart."""
    if artifact not in ("manifest", "run_results"):
        return _error("artifact must be 'manifest' or 'run_results'")
    try:
        conn = _connect()
    except Exception as e:
        return _error(f"could not connect to warehouse: {e}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM meta.dbt_artifacts WHERE name = %s", (artifact,))
            row = cur.fetchone()
    except psycopg2.errors.UndefinedTable:
        return _error(
            "meta.dbt_artifacts does not exist yet. It is populated from the dbt "
            "side after every dbt run (manifest.json / run_results.json are upserted "
            "by name). Run dbt with the artifact uploader to create it."
        )
    except Exception as e:
        return _error(str(e).strip())
    finally:
        conn.close()
    if row is None:
        return _error(
            f"no '{artifact}' row in meta.dbt_artifacts yet. dbt has not uploaded "
            "this artifact — run dbt with the artifact uploader, then retry."
        )
    data = row[0]  # jsonb comes back as a dict
    trimmed = _trim_manifest(data) if artifact == "manifest" else _trim_run_results(data)
    return json.dumps(trimmed, default=str)


@mcp.tool()
def inspect_schema(table: str) -> str:
    """Describe one warehouse table: columns with types, total row count, and 5
    sample rows. Accepts 'schema.table' or a bare table name (searched in schema
    'raw' first, then 'analytics')."""
    try:
        conn = _connect()
    except Exception as e:
        return _error(f"could not connect to warehouse: {e}")
    try:
        with conn.cursor() as cur:
            if "." in table:
                schema_name, table_name = table.split(".", 1)
                candidates = [(schema_name, table_name)]
            else:
                candidates = [("raw", table), ("analytics", table)]

            columns = None
            for schema_name, table_name in candidates:
                cur.execute(
                    """SELECT column_name, data_type, is_nullable
                       FROM information_schema.columns
                       WHERE table_schema = %s AND table_name = %s
                       ORDER BY ordinal_position""",
                    (schema_name, table_name),
                )
                columns = cur.fetchall()
                if columns:
                    break
            if not columns:
                cur.execute(
                    """SELECT table_schema || '.' || table_name
                       FROM information_schema.tables
                       WHERE table_schema IN ('raw', 'analytics')
                       ORDER BY 1"""
                )
                available = [r[0] for r in cur.fetchall()]
                return _error(
                    f"table '{table}' not found (searched: "
                    f"{', '.join(s + '.' + t for s, t in candidates)}). "
                    f"Available tables: {', '.join(available)}"
                )

            ident = sql.SQL("{}.{}").format(sql.Identifier(schema_name),
                                            sql.Identifier(table_name))
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(ident))
            row_count = cur.fetchone()[0]
            cur.execute(sql.SQL("SELECT * FROM {} LIMIT 5").format(ident))
            sample_cols = [d.name for d in cur.description]
            sample_rows = [list(r) for r in cur.fetchall()]

        return json.dumps({
            "schema": schema_name,
            "table": table_name,
            "columns": [{"name": c, "type": t, "nullable": n == "YES"}
                        for c, t, n in columns],
            "row_count": row_count,
            "sample_rows": {"columns": sample_cols, "rows": sample_rows},
        }, default=str)
    except Exception as e:
        return _error(str(e).strip())
    finally:
        conn.close()


if __name__ == "__main__":
    # Two transports: stdio for local dev (test_client.py spawns us as a child
    # process); streamable HTTP in the MintMCP-hosted container (their platform
    # probes /mcp on :8000). The Dockerfile sets MCP_TRANSPORT=http.
    if os.environ.get("MCP_TRANSPORT") == "http":
        # MintMCP's runtime assigns the port via the PORT env var (despite the
        # docs saying "serve on 8000" — their probe connects to $PORT, verified
        # in hosted-cli source). 8000 stays the fallback for manual runs.
        port = int(os.environ.get("PORT", "8000"))
        mcp.run(transport="streamable-http", host="0.0.0.0", port=port, path="/mcp")
    else:
        mcp.run()
