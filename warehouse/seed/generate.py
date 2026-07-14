"""Deterministic synthetic e-commerce seed data for the Faultline warehouse.

DETERMINISM IS THE POINT: every random draw comes from ONE shared
random.Random(42), and "now" is a fixed constant (never datetime.now()).
Two runs produce byte-identical data, which is what makes "expected metric
value" meaningful for the eval harness — the clean warehouse must be exactly
reproducible so injected faults are the only source of deviation.

Schema contract: docs/FAULTLINE_SPEC.md — 5 tables in Postgres schema `raw`,
data window 2026-04-01 .. 2026-07-10 inclusive, timestamps timezone-naive UTC.
"""

import os
import random
import sys
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Fixed clock and window (never datetime.now())
# ---------------------------------------------------------------------------
WINDOW_START = datetime(2026, 4, 1)
WINDOW_END = datetime(2026, 7, 10)
# The snapshot moment: anything (delivery, refund) that would happen after
# this instant has not happened yet, which yields a realistic in-transit /
# not-yet-shipped tail at the end of the window.
CUTOFF = datetime(2026, 7, 10, 23, 59, 59)
SIGNUP_EPOCH = datetime(2025, 1, 1)  # earliest possible customer signup

# ---------------------------------------------------------------------------
# Volumes and proportions (approximate targets from the contract)
# ---------------------------------------------------------------------------
N_CUSTOMERS = 10_000
BASE_ORDERS_PER_DAY = 316  # * 101 days ~= 32,000 orders

# Mon..Sun multipliers, mean 1.0. Sat peak / Tue trough = 1.20 / 0.87 ~= 1.38,
# inside the contracted 1.3-1.4x weekend seasonality.
WEEKDAY_WEIGHTS = [0.90, 0.87, 0.89, 0.95, 1.05, 1.20, 1.14]

P_CANCELLED = 0.06
P_STUCK_PLACED = 0.08        # never progressed to fulfillment
P_RETURNED = 0.032           # of fully delivered orders
P_DELIVERED_REFUND = 0.010   # partial refunds on kept (delivered) orders
P_FAILED_ATTEMPT = 0.042     # extra failed payment row before the real one
P_PENDING_ON_PLACED = 0.25   # some stuck orders only have a pending payment
P_SPLIT_PAYMENT = 0.03       # gift card + card
P_LATE_LOAD = 0.02           # _loaded_at up to 3 days after ordered_at

CENT = Decimal("0.01")

# Small hardcoded catalog; weights skew toward cheaper items. Basket of 1-5
# items over this distribution keeps daily revenue smooth enough that a +30%
# fault deviation stands out against a 7-day trailing average.
CATALOG_PRICES = [Decimal(p) for p in (
    "7.99", "9.99", "12.50", "14.99", "19.99", "24.99", "29.99", "34.50",
    "39.99", "45.00", "49.99", "59.99", "64.50", "74.99", "79.99", "89.99",
    "99.99", "119.00", "129.99", "149.99", "179.00", "199.99",
)]
CATALOG_WEIGHTS = [6, 8, 7, 8, 9, 8, 7, 6, 6, 5, 5, 4, 3, 3, 3, 2, 2, 2, 1, 1, 1, 1]
BASKET_SIZES = [1, 2, 3, 4, 5]
BASKET_WEIGHTS = [40, 30, 15, 10, 5]

PAYMENT_METHODS = ["card", "paypal", "gift_card", "bank_transfer"]
PAYMENT_METHOD_WEIGHTS = [65, 20, 8, 7]

CARRIERS = ["UPS", "FedEx", "USPS", "DHL"]
CARRIER_WEIGHTS = [35, 30, 25, 10]
SHIPPING_COSTS = [Decimal(c) for c in ("4.99", "5.99", "7.99", "9.99", "12.49", "14.99")]

REFUND_REASONS_RETURNED = ["changed_mind", "wrong_item", "damaged"]
REFUND_REASONS_RETURNED_W = [50, 30, 20]
REFUND_REASONS_KEPT = ["damaged", "late", "wrong_item"]
REFUND_REASONS_KEPT_W = [40, 35, 25]

# Orders skew toward morning-to-evening hours.
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 9, 10, 10, 9, 9, 9, 10, 11, 12, 11, 8, 5, 3]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Carlos", "Karen", "Daniel",
    "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Priya", "Sandra",
    "Kevin", "Ashley", "Brian", "Emily", "George", "Michelle", "Wei",
    "Amanda", "Jose", "Melissa", "Omar", "Stephanie",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Patel",
]
CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("Austin", "TX"), ("Jacksonville", "FL"), ("San Jose", "CA"),
    ("Columbus", "OH"), ("Charlotte", "NC"), ("Indianapolis", "IN"),
    ("Seattle", "WA"), ("Denver", "CO"), ("Nashville", "TN"),
    ("Portland", "OR"), ("Boston", "MA"), ("Atlanta", "GA"),
    ("Miami", "FL"), ("Minneapolis", "MN"), ("Raleigh", "NC"),
]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com"]

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.raw_customers CASCADE;
DROP TABLE IF EXISTS raw.raw_orders CASCADE;
DROP TABLE IF EXISTS raw.raw_payments CASCADE;
DROP TABLE IF EXISTS raw.raw_shipments CASCADE;
DROP TABLE IF EXISTS raw.raw_refunds CASCADE;

-- FK relationships are logical only: raw landing tables must not reject bad
-- rows, because injecting bad rows is the whole point of this project.
CREATE TABLE raw.raw_customers (
    customer_id text PRIMARY KEY,
    first_name  text,
    last_name   text,
    email       text,
    city        text,
    state       text,
    created_at  timestamp
);

CREATE TABLE raw.raw_orders (
    order_id    text PRIMARY KEY,
    customer_id text,
    ordered_at  timestamp,
    status      text,
    currency    text,
    _loaded_at  timestamp
);

CREATE TABLE raw.raw_payments (
    payment_id     text PRIMARY KEY,
    order_id       text,
    amount         numeric(10,2),
    payment_method text,
    status         text,
    paid_at        timestamp
);

-- Deliberately NO unique constraint on order_id: the downstream 1:1 join
-- assumption stays implicit so a fan-out fault can later break it silently.
CREATE TABLE raw.raw_shipments (
    shipment_id   text PRIMARY KEY,
    order_id      text,
    carrier       text,
    shipping_cost numeric(6,2),
    shipped_at    timestamp,
    delivered_at  timestamp
);

CREATE TABLE raw.raw_refunds (
    refund_id   text PRIMARY KEY,
    order_id    text,
    amount      numeric(10,2),
    reason      text,
    refunded_at timestamp
);
"""

TABLES = ["raw_customers", "raw_orders", "raw_payments", "raw_shipments", "raw_refunds"]


def get_database_url() -> str:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path)
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        sys.exit(
            f"ERROR: DATABASE_URL is missing or empty.\n"
            f"Set it in {env_path} (e.g. a Supabase session-pooler URL):\n"
            f"  DATABASE_URL=postgresql://user:pass@host:5432/postgres"
        )
    return url


def basket_total(rng: random.Random) -> Decimal:
    n_items = rng.choices(BASKET_SIZES, weights=BASKET_WEIGHTS)[0]
    prices = rng.choices(CATALOG_PRICES, weights=CATALOG_WEIGHTS, k=n_items)
    return sum(prices, Decimal("0.00"))


def order_timestamp(rng: random.Random, day: datetime) -> datetime:
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    return day + timedelta(hours=hour, minutes=rng.randrange(60), seconds=rng.randrange(60))


def generate_orders(rng: random.Random):
    """Build orders + payments + shipments + refunds in one chronological pass.

    Returns the four row lists plus {customer_index: first ordered_at}, which
    generate_customers() uses to guarantee created_at <= first order.
    """
    orders, payments, shipments, refunds = [], [], [], []
    first_order_at: dict[int, datetime] = {}
    oid = pid = sid = rid = 0

    day = WINDOW_START
    while day <= WINDOW_END:
        weight = WEEKDAY_WEIGHTS[day.weekday()]
        n_orders = int(BASE_ORDERS_PER_DAY * weight * rng.uniform(0.96, 1.04))

        for _ in range(n_orders):
            oid += 1
            order_id = f"O{oid:06d}"
            ordered_at = order_timestamp(rng, day)

            # Exponent 1.2 skews order volume toward low-index customers so a
            # few heavy repeat buyers exist (makes int_customer_ltv non-trivial).
            cidx = min(int(N_CUSTOMERS * (rng.random() ** 1.2)) + 1, N_CUSTOMERS)
            if cidx not in first_order_at or ordered_at < first_order_at[cidx]:
                first_order_at[cidx] = ordered_at

            # --- status + fulfillment timeline -----------------------------
            delivered_at = None
            shipment = None
            roll = rng.random()
            if roll < P_CANCELLED:
                status = "cancelled"
            elif roll < P_CANCELLED + P_STUCK_PLACED:
                status = "placed"
            else:
                shipped_at = ordered_at + timedelta(hours=rng.uniform(4, 48))
                transit = timedelta(hours=rng.uniform(24, 144))  # ~20% exceed the 120h "late" bar
                if shipped_at > CUTOFF:
                    status = "placed"  # not yet shipped as of the snapshot
                elif shipped_at + transit > CUTOFF:
                    status = "shipped"  # in transit: shipment row, NULL delivered_at
                    shipment = (shipped_at, None)
                else:
                    delivered_at = shipped_at + transit
                    status = "returned" if rng.random() < P_RETURNED else "delivered"
                    shipment = (shipped_at, delivered_at)

            if shipment is not None:
                sid += 1
                shipments.append((
                    f"S{sid:06d}",
                    order_id,
                    rng.choices(CARRIERS, weights=CARRIER_WEIGHTS)[0],
                    rng.choice(SHIPPING_COSTS),
                    shipment[0],
                    shipment[1],
                ))

            # --- payments ---------------------------------------------------
            total = basket_total(rng)
            if rng.random() < P_FAILED_ATTEMPT:
                pid += 1
                payments.append((
                    f"P{pid:06d}", order_id, total,
                    rng.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0],
                    "failed",
                    ordered_at + timedelta(minutes=rng.uniform(0.5, 2)),
                ))

            if status == "placed" and rng.random() < P_PENDING_ON_PLACED:
                pid += 1
                payments.append((
                    f"P{pid:06d}", order_id, total,
                    rng.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0],
                    "pending",
                    ordered_at + timedelta(minutes=rng.uniform(5, 30)),
                ))
                success_total = Decimal("0.00")
            else:
                paid_at = ordered_at + timedelta(minutes=rng.uniform(2, 45))
                if rng.random() < P_SPLIT_PAYMENT:
                    frac = Decimal(str(round(rng.uniform(0.10, 0.50), 2)))
                    gift = (total * frac).quantize(CENT, rounding=ROUND_HALF_UP)
                    pid += 1
                    payments.append((f"P{pid:06d}", order_id, gift, "gift_card", "success", paid_at))
                    pid += 1
                    payments.append((f"P{pid:06d}", order_id, total - gift, "card", "success",
                                     paid_at + timedelta(seconds=rng.uniform(5, 60))))
                else:
                    pid += 1
                    payments.append((
                        f"P{pid:06d}", order_id, total,
                        rng.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0],
                        "success", paid_at,
                    ))
                success_total = total

            # --- refunds (delivered/returned only, <= successful payments) --
            if status == "returned":
                refunded_at = delivered_at + timedelta(days=rng.uniform(2, 14))
                if refunded_at <= CUTOFF:  # refunds past the snapshot don't exist yet
                    rid += 1
                    refunds.append((
                        f"R{rid:06d}", order_id, success_total,
                        rng.choices(REFUND_REASONS_RETURNED, weights=REFUND_REASONS_RETURNED_W)[0],
                        refunded_at,
                    ))
            elif status == "delivered" and rng.random() < P_DELIVERED_REFUND:
                refunded_at = delivered_at + timedelta(days=rng.uniform(1, 10))
                if refunded_at <= CUTOFF:
                    frac = Decimal(str(round(rng.uniform(0.10, 0.50), 2)))
                    rid += 1
                    refunds.append((
                        f"R{rid:06d}", order_id,
                        (success_total * frac).quantize(CENT, rounding=ROUND_HALF_UP),
                        rng.choices(REFUND_REASONS_KEPT, weights=REFUND_REASONS_KEPT_W)[0],
                        refunded_at,
                    ))

            # --- ingestion time (~2% late-arriving, up to 3 days) -----------
            if rng.random() < P_LATE_LOAD:
                loaded_at = ordered_at + timedelta(hours=rng.uniform(24, 72))
            else:
                loaded_at = ordered_at + timedelta(minutes=rng.uniform(5, 90))
            loaded_at = min(loaded_at, CUTOFF)  # can't be loaded after the snapshot

            orders.append((order_id, f"C{cidx:06d}", ordered_at, status, "USD", loaded_at))

        day += timedelta(days=1)

    return orders, payments, shipments, refunds, first_order_at


def generate_customers(rng: random.Random, first_order_at: dict[int, datetime]):
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        city, state = rng.choice(CITIES)
        # Index suffix guarantees email uniqueness from small name lists.
        email = f"{first.lower()}.{last.lower()}{i}@{rng.choice(EMAIL_DOMAINS)}"

        first_order = first_order_at.get(i)
        if first_order is not None:
            created_at = first_order - timedelta(days=rng.uniform(0.5, 365))
        else:
            span = (WINDOW_END - SIGNUP_EPOCH).total_seconds()
            created_at = SIGNUP_EPOCH + timedelta(seconds=rng.uniform(0, span))

        rows.append((f"C{i:06d}", first, last, email, city, state, created_at))
    return rows


def load(conn, customers, orders, payments, shipments, refunds):
    inserts = [
        ("INSERT INTO raw.raw_customers "
         "(customer_id, first_name, last_name, email, city, state, created_at) VALUES %s",
         customers),
        ("INSERT INTO raw.raw_orders "
         "(order_id, customer_id, ordered_at, status, currency, _loaded_at) VALUES %s",
         orders),
        ("INSERT INTO raw.raw_payments "
         "(payment_id, order_id, amount, payment_method, status, paid_at) VALUES %s",
         payments),
        ("INSERT INTO raw.raw_shipments "
         "(shipment_id, order_id, carrier, shipping_cost, shipped_at, delivered_at) VALUES %s",
         shipments),
        ("INSERT INTO raw.raw_refunds "
         "(refund_id, order_id, amount, reason, refunded_at) VALUES %s",
         refunds),
    ]
    with conn:  # one transaction: full reset + reload, or nothing
        with conn.cursor() as cur:
            cur.execute(DDL)
            for sql, rows in inserts:
                execute_values(cur, sql, rows, page_size=1000)


def print_summary(conn):
    total = 0
    print("\nrow counts")
    print("-" * 34)
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute(f"SELECT count(*) FROM raw.{table}")
            n = cur.fetchone()[0]
            total += n
            print(f"raw.{table:<22}{n:>8,}")
    print("-" * 34)
    print(f"{'total':<26}{total:>8,}")


def main():
    url = get_database_url()

    rng = random.Random(42)  # the single shared RNG; nothing else may draw randomness
    orders, payments, shipments, refunds, first_order_at = generate_orders(rng)
    customers = generate_customers(rng, first_order_at)

    conn = psycopg2.connect(url)
    try:
        load(conn, customers, orders, payments, shipments, refunds)
        print_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
