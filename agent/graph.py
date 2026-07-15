"""Faultline investigator — a LangGraph ReAct agent that diagnoses silent
warehouse failures from a single metric-anomaly alert.

One LLM node + one tool node, looping. The
investigation *method* lives in the system prompt; the graph is plumbing.
All tools arrive through the MintMCP gateway (M2M-authenticated) — the agent
has no direct database access and never sees fault specs or ground truth.

CLI:
    uv run python agent/graph.py --alert "total_revenue in mart_revenue_daily
        is +25% vs expected, starting 2026-06-28"
"""

import argparse
import asyncio
import json
import os
import time

from dotenv import load_dotenv
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI
from langchain.agents import create_agent
from langchain.agents.structured_output import ToolStrategy
from pydantic import BaseModel, Field

from mcp_client import _env, M2MToken, REPO_ROOT 

load_dotenv(REPO_ROOT / ".env")

# Single model via OpenRouter
MODEL = os.environ.get("FAULTLINE_MODEL", "minimax/minimax-m3")
MAX_TOOL_CALLS = 25  # bounds cost; also an eval metric ("tool calls per investigation")


class Diagnosis(BaseModel):
    """Structured output — scoring.py compares this against YAML ground truth."""

    root_cause_model: str = Field(
        description="The dbt model (or raw source table) where the defect lives, "
                    "e.g. 'int_orders_joined' or 'raw_payments'")
    mechanism: str = Field(
        description="One paragraph: what is mechanically wrong and how it propagates "
                    "to the alerted metric")
    evidence: list[str] = Field(
        description="Specific query findings supporting the diagnosis, with numbers")
    proposed_fix: str = Field(description="The smallest change that corrects the metric")
    confidence: float = Field(ge=0, le=1, description="0-1 self-assessed confidence")


SYSTEM_PROMPT = """\
You are Faultline, a data-pipeline debugger. A metric anomaly alert is your ONLY
starting knowledge. The warehouse: Postgres, schema `raw` (source tables) and
`analytics` (dbt models: stg_* views, int_* views, mart_* tables). dbt runs are
GREEN — the failure is silent, so tests and run statuses will not point at it.

Method (follow it, do not improvise the order):
1. Parse the alert: affected mart, metric, direction, onset date.
2. Call get_dbt_artifacts with artifact='manifest'. Build the upstream lineage
   of the alerted mart from `depends_on`, and READ the raw_code of each model on
   the path — join shapes and WHERE clauses are where silent faults live.
3. Walk upstream hop by hop from the mart. At each model, run small diagnostic
   queries via run_query:
   - row counts per day around the onset date (compare before vs after)
   - join-key uniqueness (e.g. one row per order_id?) — fan-out inflates metrics
   - null rates on measure columns (SUM skips NULLs silently — deflates metrics)
   - value distributions (avg/max per day) — unit changes inflate by 10-100x
4. A hypothesis is CONFIRMED only when you have query evidence of the defect in
   the data or model logic, not just a plausible story. Distinguish the model
   where the defect BECOMES wrong (bad join, missing filter) from the source
   table carrying bad rows; name the model whose logic lets the fault through.
5. Stop when confirmed, or when you have exhausted upstream sources.

Constraints: read-only SQL; results cap at 200 rows, so aggregate — never
SELECT * over raw tables. Prefer a few sharp queries per hop over many broad
ones. You have a budget of {max_calls} tool calls total. Dates in the data run
2026-04-01 to 2026-07-10.
"""


async def investigate(alert: str) -> dict:
    """Run one investigation. Returns {diagnosis, tool_calls, wall_seconds, model}."""
    token = M2MToken()
    client = MultiServerMCPClient({
        "faultline": {
            "transport": "streamable_http",
            "url": _env("MINTMCP_MCP_URL"),
            # Fresh token per investigation; M2MToken re-exchanges near expiry.
            "headers": {"Authorization": f"Bearer {token.get()}"},
        }
    })
    tools = await client.get_tools()

    llm = ChatOpenAI(
        model=MODEL,
        api_key=_env("OPENROUTER_API_KEY"),
        base_url="https://openrouter.ai/api/v1",
        temperature=0,
    )
    agent = create_agent(
        llm,
        tools,
        system_prompt=SYSTEM_PROMPT.format(max_calls=MAX_TOOL_CALLS),
        # ToolStrategy: emit the Diagnosis via a forced tool call — reliable on
        # models without native json_schema response_format (e.g. via OpenRouter).
        response_format=ToolStrategy(Diagnosis),
    )

    started = time.monotonic()
    # ~2 graph steps per tool call, plus slack for the final structured answer.
    state = await agent.ainvoke(
        {"messages": [("user", f"ALERT: {alert}")]},
        config={"recursion_limit": MAX_TOOL_CALLS * 2 + 10,
                "run_name": "faultline-investigation"},
    )
    wall = time.monotonic() - started

    tool_calls = sum(
        len(getattr(m, "tool_calls", []) or []) for m in state["messages"]
    )
    diagnosis: Diagnosis | None = state.get("structured_response")
    if diagnosis is None:
        last = state["messages"][-1].content if state["messages"] else "(no messages)"
        raise RuntimeError(
            f"agent finished without a structured Diagnosis; final message: {last[:500]}"
        )
    return {
        "diagnosis": diagnosis.model_dump(),
        "tool_calls": tool_calls,
        "wall_seconds": round(wall, 1),
        "model": MODEL,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one Faultline investigation")
    ap.add_argument("--alert", required=True, help="metric anomaly alert text")
    args = ap.parse_args()
    result = asyncio.run(investigate(args.alert))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
