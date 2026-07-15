"""Faultline eval harness.

For each fault: reset -> inject -> detect -> investigate (through the gateway) ->
score against YAML ground truth -> append a row. The agent sees only the detector's
alert; the harness holds the ground truth. That asymmetry is what makes the numbers
mean something.

    uv run python harness/harness.py --all-faults
    uv run python harness/harness.py --fault silent_null_payments
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "agent"))
sys.path.insert(0, str(REPO_ROOT / "harness"))

import detector          # noqa: E402
import graph             # noqa: E402  (the investigator; imports the MCP client)
import scoring           # noqa: E402

FAULTS_DIR = REPO_ROOT / "faults"
RESULTS_PATH = REPO_ROOT / "harness" / "results.json"


def _load_specs() -> dict[str, dict]:
    specs = {}
    for path in sorted(FAULTS_DIR.glob("*.yaml")):
        spec = yaml.safe_load(path.read_text())
        specs[spec["id"]] = spec
    return specs


def _inject(*args: str) -> None:
    r = subprocess.run([sys.executable, str(FAULTS_DIR / "inject.py"), *args],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"inject.py {' '.join(args)} failed:\n{r.stderr[-800:]}")


def _retry(label: str, fn, attempts: int = 3):
    """Retry a warehouse step across a transient Supabase drop. Reset+inject is
    retried as a unit (never inject alone), so a retry always restarts from a
    clean reseed and can't double-apply a mutation."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if i == attempts - 1:
                raise
            print(f"  {label} failed ({str(e).strip()[:100]}); "
                  f"retry {i + 1}/{attempts - 1} ...", flush=True)
            time.sleep(3 * (i + 1))


def _reset_and_inject(fault_id: str) -> None:
    _inject("--reset")
    _inject("--fault", fault_id)


async def _run_one(spec: dict) -> dict:
    fault_id = spec["id"]
    print(f"  reset + inject {fault_id} ...", flush=True)
    _retry(f"reset+inject {fault_id}", lambda: _reset_and_inject(fault_id))

    alert = _retry("detect", detector.detect)
    if alert is None:
        return {"fault": fault_id, "error": "detector found no anomaly"}
    alert_text = alert.to_text()
    print(f"  alert: {alert_text}", flush=True)
    print(f"  investigating ...", flush=True)

    try:
        result = await graph.investigate(alert_text)
    except Exception as e:
        return {"fault": fault_id, "alert": alert_text, "error": f"agent: {e}"}

    s = scoring.score(result["diagnosis"], spec["ground_truth"])
    return {
        "fault": fault_id,
        "category": spec.get("category"),
        "difficulty": spec.get("difficulty"),
        "alert": alert_text,
        "diagnosis": result["diagnosis"],
        "score": s,
        "tool_calls": result["tool_calls"],
        "wall_seconds": result["wall_seconds"],
        "model": result["model"],
    }


def _print_table(rows: list[dict]) -> None:
    hdr = f"{'fault':<28}{'diff':<8}{'root_cause':<14}{'mech':<6}{'fix':<6}{'calls':<7}{'wall':<7}"
    print("\n" + hdr)
    print("-" * len(hdr))
    for r in rows:
        if r.get("error"):
            print(f"{r['fault']:<28}{'':<8}ERROR: {r['error'][:60]}")
            continue
        s = r["score"]
        rc = "PASS" if s["root_cause_correct"] else f"x({s['root_cause_got']})"
        print(f"{r['fault']:<28}{str(r['difficulty']):<8}{rc:<14}"
              f"{'PASS' if s['mechanism_correct'] else 'x':<6}"
              f"{'PASS' if s['fix_acceptable'] else 'x':<6}"
              f"{r['tool_calls']:<7}{str(r['wall_seconds'])+'s':<7}")
    scored = [r for r in rows if not r.get("error")]
    if scored:
        rc = sum(r["score"]["root_cause_correct"] for r in scored)
        mech = sum(r["score"]["mechanism_correct"] for r in scored)
        fix = sum(r["score"]["fix_acceptable"] for r in scored)
        n = len(scored)
        print("-" * len(hdr))
        print(f"root_cause {rc}/{n}   mechanism {mech}/{n}   fix {fix}/{n}   "
              f"(n={n}; {len(rows)-n} errored)")


async def main() -> None:
    ap = argparse.ArgumentParser(description="Faultline eval harness")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all-faults", action="store_true")
    g.add_argument("--fault", help="single fault id")
    args = ap.parse_args()

    specs = _load_specs()
    # Diagnostic faults only (data/sql mutation). payload_plant adversarial specs
    # are scored by the gateway-block harness, not here.
    diagnostic = [s for s in specs.values()
                  if s.get("injection", {}).get("type") in ("data_mutation", "sql_mutation")]

    if args.fault:
        if args.fault not in specs:
            sys.exit(f"unknown fault: {args.fault} (have: {', '.join(specs)})")
        targets = [specs[args.fault]]
    else:
        targets = diagnostic

    rows = []
    for spec in targets:
        print(f"\n=== {spec['id']} ===", flush=True)
        rows.append(await _run_one(spec))

    print("\nleaving warehouse clean ...", flush=True)
    _retry("final reset", lambda: _inject("--reset"))

    _print_table(rows)
    RESULTS_PATH.write_text(json.dumps(rows, indent=2, default=str))
    print(f"\nresults written to {RESULTS_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    asyncio.run(main())
