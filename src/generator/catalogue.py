"""Deterministic EzyShop catalogue: suppliers, products, inventory, campaigns and customers.

Everything is derived from a fixed random seed, so the generator, the backfill and the
Postgres seed always agree on IDs and prices without having to talk to each other.

    python -m src.generator.catalogue --seed-db        # (re)seed Postgres, idempotent
    python -m src.generator.catalogue --export DIR     # write CSV extracts for sample_data/
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import random
from dataclasses import asdict, dataclass

SEED = 20260101  # fixed: catalogue must not change between runs

# category -> (subcategories, (min_price, max_price), relative demand weight)
CATEGORIES: dict[str, tuple[list[str], tuple[float, float], float]] = {
    "electronics": (["headphones", "speakers", "tv", "camera", "smartwatch"], (39, 1499), 1.3),
    "computing":   (["laptop", "monitor", "keyboard", "mouse", "storage"], (19, 2499), 1.0),
    "phones":      (["smartphone", "case", "charger", "cable", "earbuds"], (9, 1799), 1.2),
    "apparel":     (["t-shirt", "jacket", "jeans", "dress", "hoodie"], (15, 249), 1.5),
    "footwear":    (["sneakers", "boots", "sandals", "running", "formal"], (29, 299), 0.9),
    "home":        (["bedding", "lighting", "decor", "storage-box", "rug"], (12, 399), 1.0),
    "kitchen":     (["cookware", "blender", "coffee", "knives", "kettle"], (14, 699), 0.8),
    "beauty":      (["skincare", "fragrance", "haircare", "makeup", "grooming"], (8, 189), 1.1),
}
PRODUCTS_PER_CATEGORY = 25  # 8 x 25 = 200 products

_ADJ = ["Pro", "Air", "Max", "Lite", "Plus", "Eco", "Ultra", "Classic", "Smart", "Mini", "Neo", "Prime"]
_BRANDS = ["Nordvik", "Kestrel", "Lumio", "Altura", "Brindle", "Corvo", "Halden", "Mirae", "Ostro", "Tavi"]
_COUNTRIES = ["Australia", "China", "Germany", "Vietnam", "USA", "Japan", "India", "Portugal"]
_WAREHOUSES = ["SYD-1", "MEL-1", "BNE-1"]
_CHANNELS = ["email", "social", "search", "display", "affiliate"]

CAMPAIGN_RANGE = (dt.date(2025, 1, 1), dt.date(2027, 12, 31))


@dataclass(frozen=True)
class Supplier:
    supplier_id: str
    name: str
    country: str
    lead_time_days: int
    reliability: float


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    subcategory: str
    price: float
    supplier_id: str
    popularity: float  # not stored in Postgres; drives the generator's product choice


@dataclass(frozen=True)
class Inventory:
    product_id: str
    warehouse: str
    on_hand: int
    reorder_point: int
    safety_stock: int


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    name: str
    channel: str
    category: str | None
    start_date: dt.date
    end_date: dt.date
    discount_pct: float


def black_friday(year: int) -> dt.date:
    """Friday after the fourth Thursday of November."""
    nov1 = dt.date(year, 11, 1)
    first_thu = nov1 + dt.timedelta(days=(3 - nov1.weekday()) % 7)
    return first_thu + dt.timedelta(weeks=3, days=1)


def build_suppliers() -> list[Supplier]:
    rng = random.Random(SEED)
    return [
        Supplier(f"S{i:03d}", f"{rng.choice(_BRANDS)} {rng.choice(['Trading', 'Supply', 'Group', 'Mfg'])} {i}",
                 rng.choice(_COUNTRIES), rng.randint(5, 45), round(rng.uniform(0.80, 0.99), 3))
        for i in range(1, 21)
    ]


def build_products() -> list[Product]:
    rng = random.Random(SEED + 1)
    suppliers = build_suppliers()
    products: list[Product] = []
    n = 1
    for cat, (subs, (lo, hi), _w) in CATEGORIES.items():
        for j in range(PRODUCTS_PER_CATEGORY):
            sub = subs[j % len(subs)]
            # log-uniform price so most items are cheap and a few are expensive
            price = round(lo * (hi / lo) ** rng.random(), 2)
            name = f"{rng.choice(_BRANDS)} {sub.replace('-', ' ').title()} {rng.choice(_ADJ)}"
            # Zipf-like popularity inside each category (a few best-sellers)
            popularity = round(1.0 / (1 + j) ** 0.8, 4)
            products.append(Product(f"P{n:04d}", name, cat, sub, price,
                                    rng.choice(suppliers).supplier_id, popularity))
            n += 1
    return products


def build_inventory(products: list[Product] | None = None) -> list[Inventory]:
    rng = random.Random(SEED + 2)
    products = products or build_products()
    rows = []
    for p in products:
        base = int(40 + 400 * p.popularity * CATEGORIES[p.category][2])
        safety = max(5, int(base * 0.15))
        reorder = safety + int(base * 0.35)
        # Deliberately unbalanced, as in the case study: some lines overstocked, some short
        on_hand = int(base * rng.choice([0.1, 0.3, 0.6, 1.0, 1.0, 1.4, 2.5]))
        rows.append(Inventory(p.product_id, rng.choice(_WAREHOUSES), on_hand, reorder, safety))
    return rows


def build_campaigns() -> list[Campaign]:
    """Monthly category campaigns plus the big retail events, over a fixed calendar range."""
    rng = random.Random(SEED + 3)
    cats = list(CATEGORIES)
    out: list[Campaign] = []
    n = 1
    start, end = CAMPAIGN_RANGE
    d = start
    while d <= end:
        for _ in range(rng.randint(1, 3)):
            s = d + dt.timedelta(days=rng.randint(0, 24))
            length = rng.randint(3, 10)
            cat = rng.choice(cats + [None])  # None = store-wide
            out.append(Campaign(f"CMP{n:04d}", f"{(cat or 'storewide').title()} promo {s:%b %Y}",
                                rng.choice(_CHANNELS), cat, s, s + dt.timedelta(days=length - 1),
                                float(rng.choice([5, 10, 15, 20, 25]))))
            n += 1
        d = (d.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
    for year in range(start.year, end.year + 1):
        bf = black_friday(year)
        events = [
            ("Black Friday", bf - dt.timedelta(days=3), bf + dt.timedelta(days=3), 30.0),  # BF -> Cyber Monday
            ("Boxing Day", dt.date(year, 12, 26), dt.date(year, 12, 31), 25.0),
            ("EOFY Sale", dt.date(year, 6, 15), dt.date(year, 6, 30), 20.0),
        ]
        for name, s, e, disc in events:
            out.append(Campaign(f"CMP{n:04d}", f"{name} {year}", "email", None, s, e, disc))
            n += 1
    return out


def active_campaigns(day: dt.date, campaigns: list[Campaign] | None = None) -> list[Campaign]:
    campaigns = campaigns if campaigns is not None else build_campaigns()
    return [c for c in campaigns if c.start_date <= day <= c.end_date]


def customer_ids(n: int | None = None) -> list[str]:
    n = n or int(os.getenv("NUM_CUSTOMERS", "5000"))
    return [f"C{i:05d}" for i in range(1, n + 1)]


# ------------------------------------------------------------------ Postgres seed / export
def seed_db() -> None:
    import psycopg2
    from psycopg2.extras import execute_values

    from src.common.config import get_config

    cfg = get_config()
    products = build_products()
    with psycopg2.connect(cfg.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE inventory, products, suppliers, campaigns CASCADE")
        execute_values(cur, "INSERT INTO suppliers VALUES %s",
                       [(s.supplier_id, s.name, s.country, s.lead_time_days, s.reliability)
                        for s in build_suppliers()])
        execute_values(cur, "INSERT INTO products VALUES %s",
                       [(p.product_id, p.name, p.category, p.subcategory, p.price, p.supplier_id)
                        for p in products])
        execute_values(cur, "INSERT INTO inventory (product_id, warehouse, on_hand, reorder_point, safety_stock) VALUES %s",
                       [(i.product_id, i.warehouse, i.on_hand, i.reorder_point, i.safety_stock)
                        for i in build_inventory(products)])
        execute_values(cur, "INSERT INTO campaigns VALUES %s",
                       [(c.campaign_id, c.name, c.channel, c.category, c.start_date, c.end_date, c.discount_pct)
                        for c in build_campaigns()])
        cur.execute("SELECT (SELECT count(*) FROM suppliers), (SELECT count(*) FROM products), "
                    "(SELECT count(*) FROM inventory), (SELECT count(*) FROM campaigns)")
        s, p, i, c = cur.fetchone()
    print(f"Postgres seeded: suppliers={s} products={p} inventory={i} campaigns={c}")


def export_csv(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    for name, rows in [("suppliers", build_suppliers()), ("products", build_products()),
                       ("inventory", build_inventory()), ("campaigns", build_campaigns())]:
        path = os.path.join(out_dir, f"{name}.csv")
        dicts = [asdict(r) for r in rows]
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(dicts[0]))
            w.writeheader()
            w.writerows(dicts)
        print(f"wrote {path} ({len(dicts)} rows)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed-db", action="store_true")
    ap.add_argument("--export", metavar="DIR")
    args = ap.parse_args()
    if args.seed_db:
        seed_db()
    if args.export:
        export_csv(args.export)
    if not (args.seed_db or args.export):
        ps = build_products()
        print(f"{len(ps)} products, {len(CATEGORIES)} categories, {len(build_campaigns())} campaigns")
