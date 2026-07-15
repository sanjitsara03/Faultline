"""Run dbt with credentials split out of DATABASE_URL.

dbt-postgres profiles can't take a connection URI, so this shim keeps .env's
DATABASE_URL as the single source of truth by parsing it into the PG* env vars
that profiles.yml reads.

Usage: uv run python warehouse/run_dbt.py <dbt args>, e.g. `... run_dbt.py run`
"""

import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import psycopg2
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
import dbconn  # noqa: E402  (repo-root helper: keepalive/retry Supabase connections)

PROJECT_DIR = REPO_ROOT / "warehouse" / "dbt_project"


def upload_artifacts(database_url: str, attempts: int = 3) -> None:
    """Upsert target/manifest.json + run_results.json into meta.dbt_artifacts.

    The MCP server runs on MintMCP's cloud and can't see this laptop's
    filesystem — the database is the only shared state it can read from.

    The upsert is idempotent, so a transient pooler drop mid-write is retried
    whole rather than failing the dbt run it belongs to.
    """
    for i in range(attempts):
        conn = dbconn.connect(database_url)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("create schema if not exists meta")
                    cur.execute(
                        "create table if not exists meta.dbt_artifacts ("
                        " name text primary key,"
                        " data jsonb not null,"
                        " generated_at timestamptz not null default now())"
                    )
                    for name in ("manifest", "run_results"):
                        payload = (PROJECT_DIR / "target" / f"{name}.json").read_text()
                        cur.execute(
                            "insert into meta.dbt_artifacts (name, data, generated_at)"
                            " values (%s, %s::jsonb, now())"
                            " on conflict (name) do update"
                            " set data = excluded.data, generated_at = excluded.generated_at",
                            (name, payload),
                        )
            print("uploaded manifest + run_results to meta.dbt_artifacts")
            return
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if i == attempts - 1:
                raise
            print(f"artifact upload dropped ({str(e).strip()[:80]}); retrying...")
            time.sleep(2 * (i + 1))
        finally:
            conn.close()


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set (expected in .env at repo root)")

    parsed = urlparse(url)
    os.environ["PGHOST"] = parsed.hostname or ""
    os.environ["PGPORT"] = str(parsed.port or 5432)
    # unquote: passwords in URLs are percent-encoded (e.g. %40 for @)
    os.environ["PGUSER"] = unquote(parsed.username or "")
    os.environ["PGPASSWORD"] = unquote(parsed.password or "")
    os.environ["PGDATABASE"] = parsed.path.lstrip("/")

    query = parse_qs(parsed.query)
    if "sslmode" in query:
        os.environ["PGSSLMODE"] = query["sslmode"][0]

    os.environ["DBT_PROFILES_DIR"] = str(PROJECT_DIR)

    args = sys.argv[1:]
    # match both "--project-dir X" and "--project-dir=X" forms
    if not any(a == "--project-dir" or a.startswith("--project-dir=") for a in args):
        args += ["--project-dir", str(PROJECT_DIR)]
    result = subprocess.run(["dbt", *args])

    # Upload lives here, not in inject.py: every dbt invocation refreshes the
    # cloud-visible artifacts; stale artifacts would poison the agent's map.
    dbt_command = next((a for a in args if not a.startswith("-")), None)
    if result.returncode == 0 and dbt_command in ("run", "build", "test"):
        upload_artifacts(url)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
