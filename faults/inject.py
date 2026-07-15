"""Fault injector: apply a YAML fault spec's data mutation to the raw schema.

Usage:
  uv run python faults/inject.py --fault <id>    # apply faults/<id>.yaml, then dbt run
  uv run python faults/inject.py --reset         # re-seed + dbt run (full deterministic restore)

Test infrastructure with direct DB access — the agent never sees or uses this.
The mutation runs in ONE transaction, then dbt rebuilds so marts (and the
meta.dbt_artifacts upload in run_dbt.py) reflect the faulted warehouse.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import psycopg2
import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
FAULTS_DIR = Path(__file__).resolve().parent

# Deterministic row selection: number matching rows by PK order, keep row n
# whenever floor(n*f) crosses an integer — an evenly spread every-k-th pick of
# exactly floor(N*f) rows. Never random() in SQL: reruns must hit identical
# rows so the measured expected_deviation values in the specs stay honest.
NTH_ROW = "floor({rn} * {f}) > floor(({rn} - 1) * {f})"


def sql_ident(name: str) -> str:
    """Table/column names come from YAML; allow only plain lowercase identifiers."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name or ""):
        sys.exit(f"unsafe identifier in fault spec: {name!r}")
    return name


def duplicate_rows(cur, table: str, params: dict) -> int:
    key = sql_ident(params["key_column"])
    f = float(params.get("fraction", 1.0))

    cur.execute(
        "select column_name from information_schema.columns"
        " where table_schema = 'raw' and table_name = %s order by ordinal_position",
        (table,),
    )
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        sys.exit(f"raw.{table} has no columns (does it exist?)")

    # New PKs continue the source sequence (S027403 -> S027404, ...) rather than
    # suffixing '_dup': an obviously synthetic id would give the fault away, and
    # either way the PK uniqueness test stays green — that's what keeps it silent.
    cur.execute(f"select max({key}) from raw.{table}")
    max_key = cur.fetchone()[0] or ""
    m = re.fullmatch(r"(\D*)(\d+)", max_key)
    if not m:
        sys.exit(f"cannot renumber key column {key} from max value {max_key!r}")
    prefix, digits = m.groups()
    new_key = (
        f"'{prefix}' || lpad(({int(digits)} + row_number() over (order by {key}))::text,"
        f" {len(digits)}, '0')"
    )

    select_cols = ", ".join(new_key if c == key else c for c in cols)
    cur.execute(
        f"insert into raw.{table} ({', '.join(cols)})"
        f" select {select_cols}"
        f" from (select *, row_number() over (order by {key}) as rn"
        f"       from raw.{table} where {params['filter']}) matched"
        f" where {NTH_ROW.format(rn='rn', f=f)}"
    )
    return cur.rowcount


def _update_selected(cur, table: str, params: dict, set_expr: str) -> int:
    key = sql_ident(params["key_column"])
    f = float(params.get("fraction", 1.0))
    cur.execute(
        f"update raw.{table} t set {set_expr}"
        f" from (select {key} as pk, row_number() over (order by {key}) as rn"
        f"       from raw.{table} where {params['filter']}) matched"
        f" where t.{key} = matched.pk and {NTH_ROW.format(rn='matched.rn', f=f)}"
    )
    return cur.rowcount


def null_column(cur, table: str, params: dict) -> int:
    col = sql_ident(params["column"])
    return _update_selected(cur, table, params, f"{col} = null")


def multiply_column(cur, table: str, params: dict) -> int:
    col = sql_ident(params["column"])
    factor = float(params["factor"])
    return _update_selected(cur, table, params, f"{col} = {col} * {factor}")


ACTIONS = {
    "duplicate_rows": duplicate_rows,
    "null_column": null_column,
    "multiply_column": multiply_column,
}


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd), flush=True)
    rc = subprocess.run(cmd).returncode
    if rc != 0:
        sys.exit(rc)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fault", help="fault id (faults/<id>.yaml)")
    ap.add_argument("--reset", action="store_true", help="restore clean warehouse")
    args = ap.parse_args()

    if args.reset:
        # generate.py drops and reloads every raw table byte-identically, so a
        # "reset one fault" is the same operation as resetting everything.
        run([sys.executable, str(REPO_ROOT / "warehouse" / "seed" / "generate.py")])
        run([sys.executable, str(REPO_ROOT / "warehouse" / "run_dbt.py"), "run"])
        return

    if not args.fault:
        ap.error("need --fault <id> or --reset")

    spec_path = FAULTS_DIR / f"{args.fault}.yaml"
    if not spec_path.exists():
        sys.exit(f"no fault spec at {spec_path}")
    spec = yaml.safe_load(spec_path.read_text())

    injection = spec["injection"]
    if injection["type"] != "data_mutation":
        sys.exit(f"unsupported injection type {injection['type']!r} (only data_mutation is implemented)")
    action = ACTIONS.get(injection["action"])
    if action is None:
        sys.exit(f"unknown action {injection['action']!r} (have: {', '.join(ACTIONS)})")
    table = sql_ident(injection["target"])

    load_dotenv(REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set (expected in .env at repo root)")

    conn = psycopg2.connect(url)
    try:
        with conn:  # one transaction: the whole mutation lands, or none of it
            with conn.cursor() as cur:
                affected = action(cur, table, injection["params"])
    finally:
        conn.close()
    print(f"injected {spec['id']}: {injection['action']} affected {affected} rows in raw.{table}")

    run([sys.executable, str(REPO_ROOT / "warehouse" / "run_dbt.py"), "run"])


if __name__ == "__main__":
    main()
