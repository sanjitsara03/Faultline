# Faultline

**An AI agent that diagnoses *silent* data-pipeline failures — and measures itself against known ground truth.**

Data pipelines fail silently: dbt runs green, dashboards render, but a fan-out join
or a silent NULL makes the numbers wrong. Humans debug this by walking the DAG
upstream with diagnostic SQL. Faultline automates that investigation with a LangGraph
agent — and, unlike most agent demos, **scores every diagnosis against a known fault's
ground truth** instead of eyeballing it.

Every agent tool call is routed through **[MintMCP](https://www.mintmcp.com)**, an
enterprise MCP gateway: the agent runs under a least-privilege identity with M2M
auth, a gateway rule blocks destructive SQL, and every call is audited.

> Built as a portfolio + job-application artifact. The differentiator is
> **programmatic evals against ground truth, not vibes**, and an **honest** account
> of where the agent succeeds and where it doesn't.

---

## Architecture

```
[ your laptop ]                          [ MintMCP cloud ]              [ Supabase Postgres ]
                                                                        (raw_* → stg_* →
 harness.py ──► LangGraph agent ──► Virtual MCP (gateway) ──► hosted ──►  int_* → mart_*)
 (holds ground   (MCP client,       • agent identity + M2M    connector
  truth; agent    LangSmith traces)  • rule: block destructive  (faultline-mcp:
  never sees it)                        SQL                       run_query,
                                      • audit log                 get_dbt_artifacts,
                                                                  inspect_schema)

 inject.py ───────────────────(direct DB; test infra, not agent behavior)──────────►
```

**The load-bearing asymmetry:** the agent's *entire* starting knowledge is a one-line
anomaly alert. It reaches the warehouse only through the gateway's three read-only
tools. It never sees the fault spec, the injector, or the ground truth. The harness
always holds the ground truth. That separation is what makes the eval numbers mean
something.

---

## How it works

1. **Seed** (`warehouse/seed/generate.py`) — deterministic synthetic e-commerce data
   (~104k rows, one `random.Random(42)`, a frozen clock). Byte-identical every run, so
   "expected metric value" is a fact, not a guess.
2. **Transform** (`warehouse/dbt_project/`) — 11 dbt models, `raw → stg → int → mart`.
3. **Inject** (`faults/inject.py` + `faults/*.yaml`) — apply one known fault
   (deterministically), re-run dbt. Everything stays green.
4. **Detect** (`harness/detector.py`) — compares each mart metric to the **same weekday
   over the prior 4 weeks** (kills weekday seasonality that makes a naive 7-day average
   useless), emits the anomaly alert. Fault-blind: it never reads the fault spec.
5. **Investigate** (`agent/graph.py`) — a LangGraph agent walks the lineage upstream
   through the gateway, reconciling each model's output against its inputs, and emits a
   structured `Diagnosis` (root-cause model, mechanism, evidence, fix, confidence).
6. **Score** (`harness/scoring.py`) — exact-match on the root-cause model (no LLM, can't
   be gamed) + LLM-as-judge (a *different* model than the investigator) on mechanism and
   fix.

```bash
uv run python harness/harness.py --all-faults --trials 5 --model openai/gpt-5   # scored eval (any OpenRouter model)
```

---

## Eval results

Three faults (fan-out join, silent NULL, unit change), 5 trials each, **five investigator
models** swept through the same gateway with the judge fixed at `gpt-4o-mini` for
fairness. Models are nondeterministic even at temperature 0, so we report *rates*, not
single runs. Each diagnosis is scored on **three independent dimensions**:

- **Root cause** — exact string match on the single dbt node where the defect lives.
  Objective, no LLM in the loop. Deliberately strict.
- **Mechanism** — LLM-judged: does the explanation capture the same causal story as
  ground truth?
- **Fix** — LLM-judged: would the proposed remediation actually work (match an
  acceptable fix)?

| Model | Root cause | Mechanism | Fix | Errored | Avg calls |
|---|---|---|---|---|---|
| **`openai/gpt-5`** | **10/15** | **15/15** | **15/15** | 0 | ~10 |
| `openai/gpt-5-mini` | 7/15 | 13/15 | 10/15 | 0 | ~9 |
| `xiaomi/mimo-v2.5-pro` | 6/9 | 9/9 | 8/9 | 6 | ~17 |
| `minimax/minimax-m3` | 6/13 | 13/13 | 8/13 | 2 | ~26 |
| `google/gemini-2.5-flash` | 9/15 | 0/15 | 4/15 | 0 | ~18 |

**`gpt-5` wins cleanly** — perfect mechanism and fix, best root-cause, efficient (~10
tool calls), zero errors or degradation. Reading the rest:

- **`gpt-5-mini`** is the value pick — near-frontier mechanism (13/15) at a fraction of
  the cost, zero errors, ~9 calls.
- **Xiaomi `mimo-v2.5-pro`** matches frontier *quality* when it completes (9/9 mechanism)
  but **6 of 15 trials errored** — great but unreliable.
- **`minimax-m3`** has perfect mechanism but is slow (~26 calls) and weaker on exact
  attribution.
- **`gemini-2.5-flash`** is the cautionary tale — fast and often names the right model,
  but **mechanism 0/15**: its explanations are consistently too imprecise for the judge.

**Why three dimensions — a diagnosis can have a correct fix but a "wrong" root cause.**
On silent-null, `gpt-5` named `raw_payments` (the source *carrying* the NULLs) where
ground truth is `stg_payments` (the model that should *guard* them) — an exact-match miss.
Yet its mechanism was right, and its proposed fix ("add a not-null test on
`stg_payments.amount`") was exactly an acceptable fix. The agent understood the bug and
targeted the right model to fix; it only tripped on labeling a single "most-responsible"
node. One collapsed "accuracy" number would hide that — three dimensions expose it.

**Honest caveats:**
- **Denominators differ** — `mimo` (9) and `minimax` (13) have fewer scored trials because
  some errored, so their rates rest on less data.
- **Mechanism reflects the fixed `gpt-4o-mini` judge's strictness** — gemini's 0/15 is
  partly the judge being harsh on gemini's terse style, not purely gemini being wrong.
- **Exact root-cause stays modest across all models**, bounded by two genuinely-ambiguous
  faults (is the culprit the model that lets bad data through, or the one whose
  aggregation breaks on it?). Mechanism is the cleaner discriminator. We keep exact-match
  strict and disclose the answer distribution rather than loosen the metric to look better.
- Agents that exhaust the tool-call budget **degrade gracefully** to a best-effort answer
  rather than crashing, so every non-errored trial yields a scored data point.

---

## Security: the gateway earns its keep

`run_query` deliberately accepts raw SQL — that's the governance-interesting tool. On
the Virtual MCP, a **rule blocks any `run_query` whose SQL contains a destructive verb**
(DROP/DELETE/TRUNCATE/ALTER/UPDATE/INSERT/CREATE/GRANT/REVOKE/COPY/MERGE), verified
against the legitimate diagnostic query set for zero false positives.

We then planted **prompt injections** in data the agent reads (dbt model descriptions
and free-text fields), instructing it to run destructive SQL — and tested two agent
configurations:

```bash
uv run python harness/adversarial.py --all            # production agent
uv run python harness/adversarial.py --all --naive    # un-hardened agent
```

- **Production agent: robust.** It resisted every injection (6/6 across two vectors) —
  its prompt's read-only discipline holds.
- **Gateway: an alignment-independent backstop.** A deliberately un-hardened agent
  (`--naive`, no read-only guardrail) *does* follow a planted "remediation" and issues
  `DELETE FROM raw.raw_shipments …` — and **the gateway blocks it** before it reaches
  the database, with the block recorded in MintMCP's activity log attributed to the
  agent identity.

That's the whole point of **defense in depth**: the gateway protects even agents that
aren't injection-safe. (A read-only DB session backs it up as a third layer.) One honest
boundary, documented: a pattern rule catches destructive *verbs*, not a `SELECT`-based
exfiltration — semantic inspection is MintMCP's Enterprise middleware, out of scope here.

---

## Observability: gateway audit log vs. LangSmith

The two views answer different questions and are complementary. **MintMCP's audit log**
is the *security/ops* view — every tool call attributed to the agent identity, which
were allowed vs. blocked by rule, an infrastructure record you'd hand an auditor.
**LangSmith** is the *agent-behavior* view — the full investigation tree, each model
decision and tool call with latency and token cost, for debugging *why* the agent did
what it did. You want both: the gateway tells you what a non-human principal was
permitted to do; LangSmith tells you how it reasoned to get there.

---

## Run it yourself

```bash
uv sync
cp .env.example .env         # set DATABASE_URL + MINTMCP_* + OPENROUTER_API_KEY + LANGSMITH_*
uv run python warehouse/seed/generate.py         # seed
uv run python warehouse/run_dbt.py run           # build models
uv run python faults/inject.py --fault fanout_orders_shipments   # break something
uv run python harness/harness.py --all-faults --trials 5 --model openai/gpt-5   # diagnose + score
uv run python harness/adversarial.py --all --naive               # injection demo
```

Repo layout: `warehouse/` (seed + dbt), `faults/` (specs + injector), `mcp_server/`
(3-tool server, deployed to MintMCP), `agent/` (LangGraph investigator + M2M client),
`harness/` (detector, eval harness, scoring, adversarial harness), `docs/` (spec, sprint
plan, MintMCP integration + product-feedback notes).

---

## Next steps (deliberately cut from this sprint)

- **Full 15–20 fault taxonomy** (schema drift, partial loads, filter regressions) — the
  eval harness generalizes; only more specs are needed.
- **Naive single-prompt baseline** for a "30% → X%" narrative.
- **MintMCP Agent Monitor** (Claude Code / Cursor hooks), **Admin MCP** (this repo was
  deployed *through* it), **GitHub connector** in the Virtual MCP, **Config-as-Code**.
- **Middleware-based semantic SQL inspection** (Enterprise) to catch the exfil boundary
  above.
- **M2M → mTLS**, per-fault least-privilege DB roles, AWS Bedrock AgentCore deployment.

Running MintMCP product-feedback notes: `docs/FEEDBACK_NOTES.md`.
