"""Hardened Postgres connections for Faultline's direct-DB scripts.

Supabase's session pooler occasionally drops connections; the eval harness hits
this during long seed + dbt cycles ("server closed the connection unexpectedly").
Two mitigations, centralized here so every direct-DB caller inherits them:

- TCP keepalives, so an idle gap between operations doesn't get the connection
  reaped by the pooler.
- A short connect-retry for transient blips at connection time.

Only test/harness infrastructure imports this (seed, inject, run_dbt, detector).
The agent never touches the DB directly — it goes through the MintMCP gateway.
"""

import os
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_KEEPALIVE = {
    "keepalives": 1,
    "keepalives_idle": 30,       # start probing after 30s idle
    "keepalives_interval": 10,   # probe every 10s
    "keepalives_count": 5,       # drop after 5 failed probes
    "connect_timeout": 15,
}


def database_url() -> str:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set (expected in .env at repo root)")
    return url


def connect(url: str | None = None, attempts: int = 3):
    """psycopg2 connection with keepalives; retry the connect on a transient drop."""
    url = url or database_url()
    last = None
    for i in range(attempts):
        try:
            return psycopg2.connect(url, **_KEEPALIVE)
        except psycopg2.OperationalError as e:
            last = e
            if i < attempts - 1:
                time.sleep(2 * (i + 1))
    raise last
