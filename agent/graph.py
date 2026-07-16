"""Faultline investigator — a LangGraph ReAct agent that diagnoses silent
warehouse failures from a single metric-anomaly alert.

One LLM node + one tool node, looping. The
investigation *method* lives in the system prompt.
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
from langgraph.errors import GraphRecursionError
from pydantic import BaseModel, Field

from mcp_client import _env, M2MToken, REPO_ROOT

load_dotenv(REPO_ROOT / ".env")

DEFAULT_MODEL = "minimax/minimax-m3"
MODEL = os.environ.get("FAULTLINE_MODEL", DEFAULT_MODEL)
MAX_TOOL_CALLS = 30  # bounds cost; also an eval metric ("tool calls per investigation")

# Reasoning models reject an explicit temperature override (they run at a fixed
# temperature); pass temperature only to models that accept it.
_NO_TEMPERATURE = ("gpt-5", "o1", "o3", "o4", "deepseek-r1")


def _build_llm(model: str) -> ChatOpenAI:
    kwargs = {"model": model, "api_key": _env("OPENROUTER_API_KEY"),
              "base_url": "https://openrouter.ai/api/v1",
              # Ask for token usage on streamed responses (sets
              # stream_options.include_usage, which OpenRouter honors) so
              # LangSmith records tokens/cost for the streamed agent turns.
              "stream_usage": True}
    if not any(p in model for p in _NO_TEMPERATURE):
        kwargs["temperature"] = 0
    return ChatOpenAI(**kwargs)


class Diagnosis(BaseModel):
    """Structured output — scoring.py compares this against YAML ground truth."""

    root_cause_model: str = Field(
        description="The dbt model (or raw source table) where the defect lives — "
                    "the exact node name, e.g. a stg_/int_/mart_ model or a raw_ "
                    "source table")
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
   the path — a model's SQL states the assumptions (join keys, filters, casts,
   aggregations) that its input data must satisfy for its output to be correct.
3. Walk upstream hop by hop from the mart. At each model, RECONCILE its output
   against its own inputs, comparing before vs after the onset date:
   - recompute the model's key aggregates directly from its declared inputs and
     check they still match its output; the hop where they stop reconciling at
     onset localizes the break.
   - profile each side of onset: row counts, grain (rows per business key vs the
     count of distinct keys), null rates on the columns that feed the metric, and
     the range/distribution of numeric columns. Anything that shifts at onset is
     a lead.
   - check whether the model's SQL (from step 2) makes an assumption the shifted
     data now violates.
4. A hypothesis is CONFIRMED only when you have query evidence of the defect in
   the data or model logic, not just a plausible story. The root cause is the
   SINGLE model whose own SQL OWNS the assumption the anomalous data violates —
   the model that had the responsibility and the opportunity to handle this
   condition and did not. It takes one of two shapes:
   (a) a model whose transformation logic is itself unsafe given the data — a
       join that assumes a uniqueness or grain it does not enforce, an
       aggregation that assumes a grain — so its own SQL actively produces the
       wrong output; or
   (b) a staging/cleaning model that is the designated place to validate or
       normalize a source field and forwards an out-of-contract value without the
       guard it owns (no null, range, or unit check) — the flaw is the MISSING
       guard, even though the model's mechanical SQL ran exactly as written.
   Priority: if some model's own transformation logic is unsafe, name that model
   (a). Only if every model on the path processes the value correctly by its own
   SQL semantics — a SUM that correctly skips NULLs, a cast that faithfully passes
   a value through — and the value is simply wrong from the source, attribute to
   the staging/cleaning model that should have caught or normalized it (b). In
   neither case is the root cause the raw source table (it only delivered the
   values), nor a downstream model that correctly aggregates or forwards values
   that were already wrong when they arrived.
5. Stop when confirmed, or when you have exhausted upstream sources.

Be decisive and efficient. Profile the metric's most direct inputs FIRST with a
few high-signal checks (row counts, grain, null rates, value ranges before vs
after onset) rather than broadly querying everything. As soon as one check shows
what changed at onset, follow that single thread upstream to the model whose SQL
is responsible — do not keep profiling unrelated columns, and never re-run a
query whose answer you already have. Aim to conclude in well under your budget;
commit to a diagnosis once the evidence identifies the responsible hop.

Constraints: read-only SQL; results cap at 200 rows, so aggregate — never
SELECT * over raw tables. You have a budget of {max_calls} tool calls total.
Dates in the data run 2026-04-01 to 2026-07-10.
"""


async def investigate(alert: str, model: str | None = None) -> dict:
    """Run one investigation. Returns {diagnosis, tool_calls, wall_seconds, model}."""
    model = model or os.environ.get("FAULTLINE_MODEL", DEFAULT_MODEL)
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

    llm = _build_llm(model)
    agent = create_agent(
        llm,
        tools,
        system_prompt=SYSTEM_PROMPT.format(max_calls=MAX_TOOL_CALLS),
        # ToolStrategy: emit the Diagnosis via a forced tool call — reliable on
        # models without native json_schema response_format (e.g. via OpenRouter).
        response_format=ToolStrategy(Diagnosis),
    )

    started = time.monotonic()
    # Stream (not ainvoke) so that if the agent exhausts its budget we still hold
    # the accumulated investigation and can force a best-effort answer instead of
    # crashing. ~2 graph steps per tool call, plus slack for the final answer.
    last_state = None
    degraded = False
    try:
        async for chunk in agent.astream(
            {"messages": [("user", f"ALERT: {alert}")]},
            config={"recursion_limit": MAX_TOOL_CALLS * 2 + 10,
                    "run_name": "faultline-investigation"},
            stream_mode="values",
        ):
            last_state = chunk
    except GraphRecursionError:
        degraded = True  # budget exhausted before the agent committed

    messages = (last_state or {}).get("messages", [])
    tool_calls = sum(len(getattr(m, "tool_calls", []) or []) for m in messages)

    diagnosis: Diagnosis | None = (last_state or {}).get("structured_response")
    if diagnosis is None:
        # Graceful degradation: the agent ran out of budget without committing.
        # Force a best-effort Diagnosis from the evidence it did gather.
        degraded = True
        diagnosis = await _force_diagnosis(llm, messages, alert)

    wall = time.monotonic() - started
    return {
        "diagnosis": diagnosis.model_dump(),
        "tool_calls": tool_calls,
        "wall_seconds": round(wall, 1),
        "model": model,
        "degraded": degraded,
    }


async def _force_diagnosis(llm, messages, alert: str) -> Diagnosis:
    """Squeeze a best-effort Diagnosis out of a budget-exhausted investigation.

    Flatten the trajectory to plain text (feeding raw messages risks a dangling
    tool_call the chat API rejects), then ask for the structured verdict directly.
    """
    lines = []
    for m in messages:
        for tc in (getattr(m, "tool_calls", None) or []):
            lines.append(f"CALLED {tc['name']}({tc.get('args', {})})")
        content = getattr(m, "content", "")
        if isinstance(content, str) and content.strip():
            lines.append(content.strip()[:1500])
    transcript = "\n".join(lines)[-12000:]  # keep the most recent evidence

    prompt = (
        f"ALERT: {alert}\n\nYour investigation so far (tool calls and results):\n"
        f"{transcript}\n\nYou have exhausted your investigation budget. Output your "
        "single best-effort Diagnosis now, based only on the evidence above. If "
        "uncertain, give your most likely root_cause_model and lower the confidence."
    )
    try:
        # function_calling (not the default json_schema) works across providers via
        # OpenRouter — Claude in particular doesn't support the json_schema method.
        structured = llm.with_structured_output(Diagnosis, method="function_calling")
        return await structured.ainvoke(
            [("system", SYSTEM_PROMPT.format(max_calls=MAX_TOOL_CALLS)),
             ("user", prompt)]
        )
    except Exception as e:
        return Diagnosis(
            root_cause_model="inconclusive",
            mechanism=f"Investigation did not converge within budget; forced summary failed ({e}).",
            evidence=[],
            proposed_fix="Re-run with a larger tool-call budget.",
            confidence=0.0,
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one Faultline investigation")
    ap.add_argument("--alert", required=True, help="metric anomaly alert text")
    ap.add_argument("--model", default=None,
                    help="investigator model (default: $FAULTLINE_MODEL or minimax-m3)")
    args = ap.parse_args()
    result = asyncio.run(investigate(args.alert, model=args.model))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
