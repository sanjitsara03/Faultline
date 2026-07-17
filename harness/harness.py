"""Faultline eval harness.

For each fault: reset -> inject -> detect -> investigate (through the gateway) ->
score against YAML ground truth -> append a row. The agent sees only the detector's
alert; the harness holds the ground truth.

    uv run python harness/harness.py --all-faults
    uv run python harness/harness.py --fault silent_null_payments
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
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
RESULTS_DIR = REPO_ROOT / "harness" / "results"


def _slug(model: str) -> str:
    return re.sub(r"[^\w.-]", "_", model)


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


def _infra_error(e: BaseException) -> bool:
    """Transport-shaped failures (intermittent gateway 401 on a still-valid token,
    session-open timeout, broken MCP stream). Worth one retry — the fault state is
    untouched and the agent is read-only, so a retried trial is a fresh sample.
    Model/agent failures are NOT retried; those are eval findings."""
    parts: list[str] = []

    def walk(x: BaseException) -> None:
        parts.append(f"{type(x).__name__}: {x}")
        for sub in getattr(x, "exceptions", None) or []:  # ExceptionGroup members
            walk(sub)
        if x.__cause__ is not None:
            walk(x.__cause__)

    walk(e)
    blob = " | ".join(parts)
    return any(m in blob for m in ("401", "timed out", "ReadTimeout",
                                   "ConnectTimeout", "BrokenResourceError",
                                   "TaskGroup"))


async def _one_trial(spec: dict, alert_text: str, model: str | None) -> dict:
    """One investigation on an already-injected warehouse. The agent is read-only,
    so repeated trials on the same injected state don't contaminate each other —
    that's what lets us amortize one reseed over N trials."""
    result = None
    for attempt in (1, 2):
        try:
            result = await graph.investigate(alert_text, model=model)
            break
        except Exception as e:
            if attempt == 1 and _infra_error(e):
                print(f"  transport error ({type(e).__name__}: {str(e)[:80]}); "
                      "retrying trial once ...", flush=True)
                await asyncio.sleep(5)
                continue
            # type name included because some errors stringify empty
            # (e.g. BrokenResourceError)
            return {"error": f"agent: {type(e).__name__}: {e}"}
    s = scoring.score(result["diagnosis"], spec["ground_truth"])
    return {
        "diagnosis": result["diagnosis"],
        "score": s,
        "tool_calls": result["tool_calls"],
        "wall_seconds": result["wall_seconds"],
        "degraded": result.get("degraded", False),
        "model": result["model"],
    }


def _aggregate(spec: dict, alert_text: str, trials: list[dict]) -> dict:
    from collections import Counter
    ok = [t for t in trials if "error" not in t]
    n = len(ok)
    answers = Counter(t["score"]["root_cause_got"] for t in ok)
    return {
        "fault": spec["id"],
        "model": ok[0]["model"] if ok else None,
        "judge_model": os.environ.get("FAULTLINE_JUDGE_MODEL", "openai/gpt-4o-mini"),
        "category": spec.get("category"),
        "difficulty": spec.get("difficulty"),
        "alert": alert_text,
        "expected_root_cause": ok[0]["score"]["root_cause_expected"] if ok else None,
        "n_trials": len(trials),
        "n_scored": n,
        "root_cause_rate": [sum(t["score"]["root_cause_correct"] for t in ok), n],
        "mechanism_rate": [sum(t["score"]["mechanism_correct"] for t in ok), n],
        "fix_rate": [sum(t["score"]["fix_acceptable"] for t in ok), n],
        "root_cause_answers": dict(answers),  # discloses attribution nondeterminism
        "avg_tool_calls": round(sum(t["tool_calls"] for t in ok) / n, 1) if n else None,
        "avg_wall": round(sum(t["wall_seconds"] for t in ok) / n, 1) if n else None,
        "degraded_count": sum(1 for t in ok if t.get("degraded")),
        "trials": trials,
    }


async def _run_one(spec: dict, trials: int = 1, model: str | None = None) -> dict:
    fault_id = spec["id"]
    print(f"  reset + inject {fault_id} ...", flush=True)
    _retry(f"reset+inject {fault_id}", lambda: _reset_and_inject(fault_id))

    alert = _retry("detect", detector.detect)
    if alert is None:
        return {"fault": fault_id, "error": "detector found no anomaly"}
    alert_text = alert.to_text()
    print(f"  alert: {alert_text}", flush=True)

    results = []
    for t in range(trials):
        print(f"  trial {t + 1}/{trials} ...", flush=True)
        results.append(await _one_trial(spec, alert_text, model))
    return _aggregate(spec, alert_text, results)


def _print_table(rows: list[dict]) -> None:
    hdr = (f"{'fault':<28}{'diff':<8}{'root_cause':<11}{'mech':<9}{'fix':<9}"
           f"{'calls':<7}{'wall':<7}")
    print("\n" + hdr)
    print("-" * len(hdr))
    footnotes = []
    for r in rows:
        if r.get("error"):
            print(f"{r['fault']:<28}{'':<8}ERROR: {r['error'][:60]}")
            continue
        rc = f"{r['root_cause_rate'][0]}/{r['root_cause_rate'][1]}"
        mech = f"{r['mechanism_rate'][0]}/{r['mechanism_rate'][1]}"
        fix = f"{r['fix_rate'][0]}/{r['fix_rate'][1]}"
        deg = f" ({r['degraded_count']} forced)" if r.get("degraded_count") else ""
        print(f"{r['fault']:<28}{str(r['difficulty']):<8}{rc:<11}{mech:<9}{fix:<9}"
              f"{str(r['avg_tool_calls']):<7}{str(r['avg_wall'])+'s':<7}{deg}")
        # disclose attribution variance: which models the agent named across trials
        answers = r.get("root_cause_answers", {})
        if len(answers) > 1 or (answers and r["root_cause_rate"][0] < r["root_cause_rate"][1]):
            dist = ", ".join(f"{k}×{v}" for k, v in sorted(answers.items(), key=lambda kv: -kv[1]))
            footnotes.append(f"  {r['fault']}: expected {r['expected_root_cause']}; "
                             f"agent named {{{dist}}} across {r['n_scored']} trials")
    scored = [r for r in rows if not r.get("error")]
    if scored:
        def tot(k):
            return (sum(r[k][0] for r in scored), sum(r[k][1] for r in scored))
        rc, mech, fix = tot("root_cause_rate"), tot("mechanism_rate"), tot("fix_rate")
        print("-" * len(hdr))
        print(f"root_cause {rc[0]}/{rc[1]}   mechanism {mech[0]}/{mech[1]}   "
              f"fix {fix[0]}/{fix[1]}   (across {len(scored)} faults)")
    if footnotes:
        print("\nroot-cause attribution across trials (exact-match; nondeterministic):")
        print("\n".join(footnotes))


async def main() -> None:
    ap = argparse.ArgumentParser(description="Faultline eval harness")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all-faults", action="store_true")
    g.add_argument("--fault", help="single fault id")
    ap.add_argument("--trials", type=int, default=1,
                    help="investigations per fault; >1 reports pass RATES "
                         "(the model is nondeterministic, so a single run is noisy)")
    ap.add_argument("--model", default=None,
                    help="investigator model (default: $FAULTLINE_MODEL or gpt-5.6-luna); "
                         "when set, results write to harness/results/<model>.json")
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

    # A named --model run writes its own file so a multi-model sweep doesn't
    # clobber; the default run keeps the canonical results.json.
    if args.model:
        RESULTS_DIR.mkdir(exist_ok=True)
        out_path = RESULTS_DIR / f"{_slug(args.model)}.json"
    else:
        out_path = RESULTS_PATH

    rows = []
    for spec in targets:
        print(f"\n=== {spec['id']} ({args.trials} trial(s)) ===", flush=True)
        rows.append(await _run_one(spec, trials=args.trials, model=args.model))
        # Checkpoint after every fault so an interrupted run keeps its progress.
        out_path.write_text(json.dumps(rows, indent=2, default=str))
        print(f"  checkpointed {len(rows)}/{len(targets)} -> {out_path.relative_to(REPO_ROOT)}", flush=True)

    print("\nleaving warehouse clean ...", flush=True)
    _retry("final reset", lambda: _inject("--reset"))

    _print_table(rows)
    out_path.write_text(json.dumps(rows, indent=2, default=str))
    print(f"\nresults written to {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    asyncio.run(main())
