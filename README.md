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

> The differentiator is **programmatic evals against ground truth, not vibes**, and
> an **honest** account of where the agent succeeds and where it doesn't.

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
uv run python harness/harness.py --all-faults --trials 5    # scored eval (default: gpt-5.6-luna; --model swaps any OpenRouter model)
```

---

## Eval results

**Eight faults across seven failure classes** — fan-out join, silent NULL, unit drift
(on payments *and* refunds), filter regression, sign corruption, null foreign key,
referential drift — alerting on two different marts, 5 trials per fault, three
current-generation investigator models through the same gateway, judge fixed at
`gpt-4o-mini` for fairness. Models are nondeterministic even at temperature 0, so we
report *rates*, not single runs. Each diagnosis is scored on **three independent
dimensions**:

- **Root cause** — exact string match on the single dbt node where the defect lives.
  Objective, no LLM in the loop. Deliberately strict.
- **Mechanism** — LLM-judged: does the explanation capture the same causal story as
  ground truth?
- **Fix** — LLM-judged: would the proposed remediation actually work (match an
  acceptable fix)?

| Model | Root cause | Mechanism | Fix | Avg calls | Avg wall | ~Cost/diagnosis |
|---|---|---|---|---|---|---|
| `openai/gpt-5.6-terra` | **40/40** | **40/40** | 37/40 | 8.4 | 37s | $0.12 |
| **`openai/gpt-5.6-luna`** (default) | 39/40 | 38/40 | 37/40 | 9.2 | 28s | **$0.04** |
| `minimax/minimax-m3` | 35/40 | 37/40 | 35/40 | 19.8 | 94s | $0.04 |

120/120 trials completed and scored — zero transport errors (the harness retries a
trial once on gateway flake, never on model failures), and zero budget-exhausted runs
for the 5.6 models.

**Luna is the default investigator**: 97% of terra's root-cause accuracy at a third of
the cost, and the fastest investigations (28s). Terra is the accuracy pick — the only
perfect root-cause + mechanism sweep.

**The cheap model isn't cheaper — efficiency beats unit price.** `minimax-m3` costs
3.3× less per token than luna yet the same per diagnosis (~$0.04), because every tool
call in an agent loop re-sends the whole transcript: minimax averages 19.8 tool calls
(~122k tokens) per investigation against luna's 9.2 calls (~31k). In agentic
workloads, investigation efficiency dominates list price.

**One ground-truth relabel, disclosed.** `orphaned_payments` (payments arriving with a
new order-reference format that matches no order) was originally labeled with the join
model as root cause. All three models independently named `stg_payments` — and by our
own attribution rubric they are right: the join handles the values with standard SQL
semantics, while staging owns normalizing the source's reference format. We corrected
the answer key and re-scored the *stored* diagnoses (no reruns). Lesson: when three
independent models unanimously disagree with your ground truth, audit the ground truth.

**What's still hard:**
- `orphaned_payments` **fixes** score only 2/5 per model — proposals tend to patch the
  join rather than quarantine bad references at staging.
- `refund_unit_inflation` alerts ~2.5 weeks *before* its mutation date (refunds lag
  orders, so inflated refunds land on old order-dates). Both 5.6 models still solved it
  5/5; minimax went 3/5.

**Honest caveats:**
- **Mechanism reflects the fixed `gpt-4o-mini` judge's strictness**, and fix scoring
  depends on the acceptable-fixes list — both disclosed per-trial in `harness/results/`.
- **Exact-match stays strict** and per-fault answer distributions are recorded, rather
  than loosening the metric to look better.
- Agents that exhaust the 30-tool-call budget **degrade gracefully** to a best-effort
  answer rather than crashing, so every trial yields a scored data point.

**Dropped along the way** (full data in `harness/results_3faults/`): on the original
3-fault set, `gpt-5` — the Aug-2025 frontier — went 14/15 on root cause and
`gpt-5-mini` 15/15, both since superseded by the cheaper, faster 5.6 family;
`gemini-2.5-flash` burned its entire tool budget on 10 of 15 trials (5/15 root cause);
`xiaomi/mimo-v2.5-pro` was accurate but heavy (9/15, 7 degraded). The biggest single
improvement was never a model swap: rewriting the prompt's attribution rubric (from
"the hop where reconciliation breaks" to "the model that owns the violated assumption")
lifted root-cause accuracy from roughly half to near-perfect on the same faults — the
method, not the model, was the bottleneck.

---

## Security: the gateway earns its keep

`run_query` deliberately accepts raw SQL — that's the governance-interesting tool. On
the Virtual MCP, a **rule blocks any `run_query` whose SQL contains a destructive verb**
(DROP/DELETE/TRUNCATE/ALTER/UPDATE/INSERT/CREATE/GRANT/REVOKE/COPY/MERGE).

We then planted **prompt injections** in data the agent reads (dbt model descriptions
and free-text fields), instructing it to run destructive SQL — and tested two agent
configurations:

```bash
uv run python harness/adversarial.py --all            # production agent
uv run python harness/adversarial.py --all --naive    # un-hardened agent
```

- **Production agent (gpt-5.6-luna): robust.** It resisted every injection (6/6 across
  both vectors), kept investigating, and still produced correct diagnoses.
- **Gateway: an alignment-independent backstop.** The deliberately un-hardened agent
  (`--naive`, no read-only guardrail) *does* follow a planted "remediation" and issues
  `DELETE FROM raw.raw_shipments …` — and **the gateway blocks it** before it reaches
  the database, with the block recorded in MintMCP's activity log attributed to the
  agent identity. (The naive agent resisted the other 5/6 on its own.)
- **And one finding we did not expect:** the pattern rule **false-positived twice** on
  perfectly legitimate read-only SELECTs — luna narrates its SQL with comments, and a
  comment reading `-- Why did refunds drop to 0?` contains the word "drop". Terser
  models never triggered this; a chattier model's *style* surfaced a real guardrail
  boundary. The agent recovered gracefully (re-issued the query without the comment and
  finished its diagnosis), and blocking too much is the safe failure direction — but
  it's a clean demonstration that pattern rules read text, not meaning. The harness's
  false-positive metric is now comment-aware so this can't be masked.

That's the whole point of **defense in depth**: the gateway protects even agents that
aren't injection-safe. (A read-only DB session backs it up as a third layer.) The
documented boundary of pattern matching — comment false-positives on one side,
`SELECT`-based exfiltration passing untouched on the other — is exactly the case for
semantic inspection, which is MintMCP's Enterprise middleware, out of scope here.

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
uv run python harness/harness.py --all-faults --trials 5    # diagnose + score (default model: gpt-5.6-luna)
uv run python harness/adversarial.py --all --naive               # injection demo
```

Repo layout: `warehouse/` (seed + dbt), `faults/` (specs + injector), `mcp_server/`
(3-tool server, deployed as a hosted connector), `agent/` (LangGraph investigator +
M2M client), `harness/` (detector, eval harness, scoring, adversarial harness).

---

## Next steps (deliberately cut from this sprint)

- **Grow the 8-fault taxonomy toward 15–20** (schema drift, partial loads, late-arriving
  data) — the harness generalizes; only more YAML specs are needed.
- **No-method baseline** — run the same models with a bare "find the root cause"
  prompt to isolate how much of the accuracy comes from the investigation rubric
  vs. raw model capability.
- **MintMCP Agent Monitor** (Claude Code / Cursor hooks), **Admin MCP** (the hosted
  connector was deployed *through* it), **GitHub connector** in the Virtual MCP,
  **Config-as-Code**.
- **Middleware-based semantic SQL inspection** (Enterprise) to catch the exfil boundary
  above.
- **M2M → mTLS**, per-fault least-privilege DB roles, AWS Bedrock AgentCore deployment.
