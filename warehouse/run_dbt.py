"""Run dbt with credentials split out of DATABASE_URL.

dbt-postgres profiles can't take a connection URI, so this shim keeps .env's
DATABASE_URL as the single source of truth by parsing it into the PG* env vars
that profiles.yml reads.

Usage: uv run python warehouse/run_dbt.py <dbt args>, e.g. `... run_dbt.py run`
"""

import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECT_DIR = REPO_ROOT / "warehouse" / "dbt_project"


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
    os.execvp("dbt", ["dbt", *args])


if __name__ == "__main__":
    main()
