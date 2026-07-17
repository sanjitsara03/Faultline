"""Score an agent Diagnosis against a fault's YAML ground truth.

Three dimensions:
- root_cause_model: exact match after normalizing node names (the hard, objective
  metric — no LLM in the loop, so it can't be gamed).
- mechanism: LLM-as-judge — does the explanation capture the same causal story?
- proposed_fix: LLM-as-judge against the list of acceptable_fixes.

The judge is a DIFFERENT model from the investigator (avoids a model grading its
own work / self-preference bias); set FAULTLINE_JUDGE_MODEL to override.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

JUDGE_MODEL = os.environ.get("FAULTLINE_JUDGE_MODEL", "openai/gpt-4o-mini")


def normalize_model(name: str) -> str:
    """dbt node names arrive in several shapes: 'int_orders_joined',
    'model.faultline.int_orders_joined', 'analytics.int_orders_joined'. Reduce to
    the bare node name for exact-match comparison."""
    return (name or "").strip().strip("`\"'").split(".")[-1].lower()


def _judge(instruction: str) -> dict:
    """Ask the judge model for a strict JSON verdict. Returns {} on failure so a
    judge outage degrades to 'unscored', never a false pass."""
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    try:
        resp = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={
                "model": JUDGE_MODEL,
                "temperature": 0,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": "You are a strict grader. "
                     "Respond ONLY with the requested JSON object."},
                    {"role": "user", "content": instruction},
                ],
            },
            timeout=90,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start, end = content.find("{"), content.rfind("}")
        return json.loads(content[start:end + 1])
    except Exception as e:
        return {"_error": str(e)[:200]}


def score(diagnosis: dict, ground_truth: dict) -> dict:
    """Compare one diagnosis against one fault's ground_truth block."""
    gt_model = ground_truth.get("root_cause_model", "")
    agent_model = diagnosis.get("root_cause_model", "")
    root_cause_correct = normalize_model(agent_model) == normalize_model(gt_model)

    mech = _judge(
        f"Ground-truth root-cause mechanism:\n{ground_truth.get('root_cause_mechanism','')}\n\n"
        f"Agent's identified root_cause_model: {agent_model}\n"
        f"Agent's mechanism explanation:\n{diagnosis.get('mechanism','')}\n\n"
        "Does the agent's explanation identify the SAME underlying causal mechanism "
        "as the ground truth (regardless of wording)? Reply JSON: "
        '{"correct": true|false, "score": 0.0-1.0, "reason": "one sentence"}'
    )

    fixes = ground_truth.get("acceptable_fixes", [])
    fix = _judge(
        f"Acceptable fixes (any one is correct):\n" +
        "\n".join(f"- {f}" for f in fixes) +
        f"\n\nAgent's proposed fix:\n{diagnosis.get('proposed_fix','')}\n\n"
        "Does the agent's proposed fix match the spirit of at least one acceptable "
        'fix? Reply JSON: {"acceptable": true|false, "reason": "one sentence"}'
    )

    return {
        "root_cause_correct": root_cause_correct,
        "root_cause_expected": normalize_model(gt_model),
        "root_cause_got": normalize_model(agent_model),
        "mechanism_correct": bool(mech.get("correct", False)),
        "mechanism_score": mech.get("score"),
        "mechanism_reason": mech.get("reason") or mech.get("_error"),
        "fix_acceptable": bool(fix.get("acceptable", False)),
        "fix_reason": fix.get("reason") or fix.get("_error"),
    }
