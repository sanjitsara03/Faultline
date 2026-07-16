"""Metric-anomaly detector — turns a faulted warehouse into the single alert
that is the investigating agent's entire starting knowledge.

This is harness/test infrastructure: it has direct DB access and knows the metric
catalog, but it NEVER reads fault specs.
"""

from __future__ import annotations

import os
import statistics
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
import dbconn  # noqa: E402  (repo-root helper: keepalive/retry Supabase connections)

# (mart table, metric column, human label). The detector is fault-blind: it scans
# this whole catalog and reports the most significant anomaly, without any idea
# which metric a given fault is "supposed" to move.
METRIC_CATALOG = [
    ("mart_revenue_daily", "total_revenue", "total_revenue"),
    ("mart_revenue_daily", "net_revenue", "net_revenue"),
    ("mart_revenue_daily", "orders_count", "orders_count"),
    ("mart_revenue_daily", "aov", "aov"),
    ("mart_customer_health", "active_customers", "active_customers"),
    ("mart_ops_fulfillment", "shipments_count", "shipments_count"),
]

LOOKBACK_WEEKS = 4     # same-weekday baseline window
Z_THRESHOLD = 3.0      # sustained |z| above this = anomaly
MIN_RUN_DAYS = 2       # require a sustained run, not a one-day blip
GAP_TOLERANCE = 2      # a run survives up to this many sub-threshold days
                       # (a real fault dips below threshold on noisy days but
                       # stays broken; without tolerance the run fragments and
                       # the reported onset jumps to a later sub-run)


@dataclass
class Alert:
    mart: str
    metric: str
    direction: str          # "up" | "down"
    deviation_pct: float     # mean signed % deviation over the anomalous run
    onset: date
    peak_z: float

    def to_text(self) -> str:
        sign = "+" if self.deviation_pct >= 0 else ""
        return (f"{self.metric} in {self.mart} is {sign}{self.deviation_pct:.0f}% vs the "
                f"same-weekday trailing average, starting {self.onset.isoformat()}")


def _database_url() -> str:
    load_dotenv(REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set (expected in .env)")
    return url


def _load_series(cur, mart: str, metric: str) -> list[tuple[date, float]]:
    # date_day is the grain of every mart in the catalog.
    cur.execute(f"SELECT date_day, {metric} FROM analytics.{mart} ORDER BY date_day")
    return [(r[0], float(r[1])) for r in cur.fetchall() if r[1] is not None]


def _scan_metric(series: list[tuple[date, float]]) -> Alert | None:
    """Same-weekday-vs-prior-4-weeks anomaly scan of one metric series."""
    by_day = {d: v for d, v in series}
    residuals: list[tuple[date, float]] = []  # (day, signed % deviation from same-weekday baseline)

    for d, actual in series:
        peers = [by_day[d - timedelta(weeks=w)]
                 for w in range(1, LOOKBACK_WEEKS + 1)
                 if d - timedelta(weeks=w) in by_day]
        if len(peers) < LOOKBACK_WEEKS:
            continue  # not enough history yet (warm-up)
        baseline = statistics.mean(peers)
        if baseline == 0:
            continue
        residuals.append((d, (actual - baseline) / baseline))

    if len(residuals) < 10:
        return None

    # Noise scale from the median-centered residuals (robust to the anomaly itself
    # sitting in the same series — a few large fault days won't inflate the scale
    # the way a plain std would).
    values = [r for _, r in residuals]
    med = statistics.median(values)
    mad = statistics.median([abs(v - med) for v in values]) or 1e-9
    sigma = 1.4826 * mad  # MAD -> std for a normal distribution

    # Dominant direction: the sign carrying the most total breach magnitude.
    breaches = [(d, v) for d, v in residuals if abs((v - med) / sigma) > Z_THRESHOLD]
    if not breaches:
        return None
    up_mag = sum(v for _, v in breaches if v > med)
    down_mag = -sum(v for _, v in breaches if v < med)
    up = up_mag >= down_mag

    def is_hot(v: float) -> bool:
        return ((v - med) / sigma > Z_THRESHOLD) if up else ((v - med) / sigma < -Z_THRESHOLD)

    # Walk chronologically, growing one run across up to GAP_TOLERANCE cool days,
    # and keep the run with the most hot days (= the sustained fault period).
    best_hot: list[tuple[date, float]] = []
    cur_hot: list[tuple[date, float]] = []
    gap = 0
    for d, v in residuals:
        if is_hot(v):
            cur_hot.append((d, v)); gap = 0
        else:
            gap += 1
            if gap > GAP_TOLERANCE:
                if len(cur_hot) > len(best_hot):
                    best_hot = cur_hot
                cur_hot = []; gap = 0
    if len(cur_hot) > len(best_hot):
        best_hot = cur_hot

    if len(best_hot) < MIN_RUN_DAYS:
        return None
    days = [d for d, _ in best_hot]
    devs = [v for _, v in best_hot]
    return Alert(
        mart="", metric="",
        direction="up" if up else "down",
        deviation_pct=100 * statistics.mean(devs),
        onset=min(days),                                   # true start of the run
        peak_z=max(abs((v - med) / sigma) for v in devs),
    )


def detect() -> Alert | None:
    """Scan the whole metric catalog; return the single most significant anomaly."""
    conn = dbconn.connect(_database_url())
    try:
        with conn.cursor() as cur:
            best: Alert | None = None
            for mart, metric, label in METRIC_CATALOG:
                series = _load_series(cur, mart, metric)
                hit = _scan_metric(series)
                if hit is None:
                    continue
                hit.mart, hit.metric = mart, label
                if best is None or hit.peak_z > best.peak_z:
                    best = hit
            return best
    finally:
        conn.close()


def main() -> None:
    alert = detect()
    if alert is None:
        print("no anomaly detected (warehouse looks clean)")
    else:
        print(alert.to_text())
        print(f"  [peak_z={alert.peak_z:.1f}, direction={alert.direction}]")


if __name__ == "__main__":
    main()
