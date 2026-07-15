"""MCP client plumbing for the Faultline investigator.

Two jobs, nothing else:
1. M2M token exchange (OAuth client-credentials) against MintMCP. Tokens are
   short-lived; we re-exchange near expiry. Every exchange is an audit event
   on the gateway — that visibility is a feature, not overhead.
2. Open an authenticated MCP session to the agent's Virtual MCP endpoint.

The LangGraph agent gets its tools through this session; nothing in this file
knows about the warehouse. Run directly for a smoke test of the full chain:
    uv run python agent/mcp_client.py
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

_REQUIRED = ("MINTMCP_TOKEN_URL", "MINTMCP_MCP_URL",
             "MINTMCP_CLIENT_ID", "MINTMCP_CLIENT_SECRET")


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set — expected all of {_REQUIRED} in {REPO_ROOT / '.env'}"
        )
    return value


class M2MToken:
    """Caches one access token; re-exchanges when <60s of life remains."""

    def __init__(self) -> None:
        self._token: str | None = None
        self._expires_at = 0.0

    def get(self) -> str:
        if self._token is None or time.time() > self._expires_at - 60:
            resp = httpx.post(
                _env("MINTMCP_TOKEN_URL"),
                data={
                    "grant_type": "client_credentials",
                    "client_id": _env("MINTMCP_CLIENT_ID"),
                    "client_secret": _env("MINTMCP_CLIENT_SECRET"),
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            self._token = payload["access_token"]
            # default matches MintMCP's documented 3600s if expires_in is absent
            self._expires_at = time.time() + float(payload.get("expires_in", 3600))
        return self._token


@asynccontextmanager
async def open_session(token: M2MToken):
    """Authenticated MCP session to the agent's VMCP (streamable HTTP)."""
    headers = {"Authorization": f"Bearer {token.get()}"}
    async with streamablehttp_client(_env("MINTMCP_MCP_URL"), headers=headers) as (
        read, write, _get_session_id,
    ):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


async def _smoke_test() -> None:
    token = M2MToken()
    async with open_session(token) as session:
        tools = await session.list_tools()
        names = sorted(t.name for t in tools.tools)
        print(f"tools via gateway: {names}")

        # VMCPs namespace tool names by connector: faultline__run_query
        result = await session.call_tool(
            "faultline__run_query",
            {"sql": "select date_day, total_revenue from analytics.mart_revenue_daily "
                    "order by date_day desc limit 3"},
        )
        payload = json.loads(result.content[0].text)
        print("run_query via gateway:")
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    asyncio.run(_smoke_test())
