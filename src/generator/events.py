"""Event construction shared by the live producer, the backfill and the tests.

The raw customer_id exists only here, inside the generator. Every emitted event carries
customer_pid = sha256(salt + customer_id) instead (pseudonymisation at ingestion).
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import math
import random
import uuid
from dataclasses import dataclass

from src.common.pseudonymise import pseudonymise
from src.generator import catalogue as cat
from src.generator.text_gen import rating_from_mood, review_text

DEVICES = ["mobile", "mobile", "mobile", "desktop", "desktop", "tablet"]
REFERRERS = ["direct", "google", "instagram", "facebook", "email", "tiktok", "affiliate"]
PAYMENTS = ["card", "card", "card", "paypal", "afterpay", "apple_pay", "gift_card"]

# Hour-of-day demand curve (UTC hours treated as shop-local for simplicity): low at night,
# lunchtime bump, evening peak.
HOURLY = [0.25, 0.15, 0.10, 0.08, 0.08, 0.12, 0.30, 0.55, 0.75, 0.85, 0.95, 1.05,
          1.15, 1.05, 0.95, 0.95, 1.00, 1.10, 1.30, 1.50, 1.55, 1.35, 0.95, 0.55]
_HOURLY_MEAN = sum(HOURLY) / 24


def hour_factor(hour: int) -> float:
    return HOURLY[hour] / _HOURLY_MEAN


def iso(ts: dt.datetime) -> str:
    return ts.astimezone(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Customer:
    customer_id: str          # raw ID: never leaves the generator
    customer_pid: str         # pseudonym that is emitted
    activity: float           # relative shopping frequency (log-normal)
    device: str
    mood: float               # baseline satisfaction in [-1, 1]


class World:
    """Deterministic catalogue + customer population."""

    def __init__(self, salt: str, n_customers: int, seed: int = 42):
        self.rng = random.Random(seed)
        self.products = cat.build_products()
        self.campaigns = cat.build_campaigns()
        self.by_cat: dict[str, list[cat.Product]] = {}
        for p in self.products:
            self.by_cat.setdefault(p.category, []).append(p)
        self.cat_names = list(cat.CATEGORIES)
        self.cat_weights = [cat.CATEGORIES[c][2] for c in self.cat_names]
        self.customers = [
            Customer(cid, pseudonymise(cid, salt), self.rng.lognormvariate(0, 0.8),
                     self.rng.choice(DEVICES), max(-1.0, min(1.0, self.rng.gauss(0.35, 0.35))))
            for cid in cat.customer_ids(n_customers)
        ]
        # cumulative weights -> O(log n) sampling (plain weights= is O(n) per draw)
        self.cust_cum = list(itertools.accumulate(c.activity for c in self.customers))
        self.cat_cum = list(itertools.accumulate(self.cat_weights))
        self.prod_cum = {k: list(itertools.accumulate(p.popularity for p in v)) for k, v in self.by_cat.items()}
        self._camp_cache: dict[dt.date, list[cat.Campaign]] = {}

    def pick_customer(self, rng: random.Random) -> Customer:
        return rng.choices(self.customers, cum_weights=self.cust_cum)[0]

    def pick_category(self, rng: random.Random) -> str:
        return rng.choices(self.cat_names, cum_weights=self.cat_cum)[0]

    def pick_product(self, rng: random.Random, category: str | None = None) -> cat.Product:
        category = category or self.pick_category(rng)
        return rng.choices(self.by_cat[category], cum_weights=self.prod_cum[category])[0]

    def active_on(self, day: dt.date) -> list[cat.Campaign]:
        if day not in self._camp_cache:
            self._camp_cache[day] = cat.active_campaigns(day, self.campaigns)
        return self._camp_cache[day]

    def campaign_for(self, rng: random.Random, day: dt.date, category: str) -> cat.Campaign | None:
        live = [c for c in self.active_on(day) if c.category in (None, category)]
        return rng.choice(live) if live and rng.random() < 0.6 else None


# ----------------------------------------------------------------------------- event builders
def click_event(c: Customer, session_id: str, product: cat.Product, action: str,
                ts: dt.datetime, rng: random.Random) -> dict:
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_time": iso(ts),
        "customer_pid": c.customer_pid,
        "session_id": session_id,
        "page": f"/p/{product.product_id}" if action == "view" else "/cart",
        "product_id": product.product_id,
        "action": action,
        "device": c.device,
        "referrer": rng.choice(REFERRERS),
    }


def order_event(world: World, c: Customer, main: cat.Product, ts: dt.datetime,
                rng: random.Random, qty_scale: float = 1.0) -> dict:
    items = [main]
    for _ in range(rng.choices([0, 1, 2], weights=[60, 30, 10])[0]):
        items.append(world.pick_product(rng, main.category if rng.random() < 0.7 else None))
    camp = world.campaign_for(rng, ts.date(), main.category)
    disc = (camp.discount_pct / 100) if camp else 0.0
    lines = []
    for p in items:
        qty = max(1, int(round(rng.choices([1, 1, 1, 2, 3], k=1)[0] * qty_scale)))
        lines.append({"product_id": p.product_id, "qty": qty, "unit_price": round(p.price * (1 - disc), 2)})
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_time": iso(ts),
        "customer_pid": c.customer_pid,
        "order_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "items": lines,
        "total": round(sum(li["qty"] * li["unit_price"] for li in lines), 2),
        "payment_method": rng.choice(PAYMENTS),
        "campaign_id": camp.campaign_id if camp else None,
    }


def review_event(c: Customer, product: cat.Product, ts: dt.datetime, rng: random.Random,
                 mood_shift: float = 0.0) -> dict:
    rating = rating_from_mood(max(-1.0, min(1.0, c.mood + mood_shift)), rng)
    title, text, _kind = review_text(rating, product.category, rng)
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_time": iso(ts),
        "customer_pid": c.customer_pid,
        "product_id": product.product_id,
        "rating": rating,
        "title": title,
        "text": text,
    }


def session_events(world: World, now: dt.datetime, rng: random.Random) -> list[tuple[str, dict]]:
    """One browsing session: views -> (maybe) add_to_cart -> (maybe) order -> (rarely) review.

    Funnel rates are typical e-commerce orders of magnitude: ~30% of sessions add to cart,
    ~40% of carts convert, ~6% of orders leave a review (raised to keep live sentiment visible).
    """
    c = world.pick_customer(rng)
    session_id = uuid.UUID(int=rng.getrandbits(128), version=4).hex[:16]
    category = world.pick_category(rng)
    out: list[tuple[str, dict]] = []
    t = now
    viewed = []
    for _ in range(max(1, int(rng.expovariate(1 / 3)))):
        p = world.pick_product(rng, category)
        viewed.append(p)
        out.append(("clicks", click_event(c, session_id, p, "view", t, rng)))
        t += dt.timedelta(milliseconds=rng.randint(200, 3000))
    if rng.random() < 0.30:
        p = rng.choice(viewed)
        out.append(("clicks", click_event(c, session_id, p, "add_to_cart", t, rng)))
        if rng.random() < 0.10:
            out.append(("clicks", click_event(c, session_id, p, "remove_from_cart", t, rng)))
        elif rng.random() < 0.40:
            out.append(("orders", order_event(world, c, p, t, rng)))
            if rng.random() < 0.06:
                out.append(("reviews", review_event(c, p, t, rng)))
    # Reviews of earlier purchases arrive independently of today's session. Without this,
    # reviews were only ~0.1% of events (first test run), too sparse for per-minute sentiment.
    if rng.random() < 0.03:
        out.append(("reviews", review_event(c, world.pick_product(rng), t, rng)))
    # The session is emitted all at once, so shift it to END at `now`. Timestamps stepped
    # forward from `now` put events up to ~20 s in the future (TEST_LOG T03), which made
    # measured freshness negative.
    shift = t - now
    if shift.total_seconds() > 0:
        for _topic, ev in out:
            ts = dt.datetime.fromisoformat(ev["event_time"].replace("Z", "+00:00")) - shift
            ev["event_time"] = iso(ts)
    return out


def to_json(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def seasonal_factor(day: dt.date) -> float:
    """Yearly seasonality: Nov/Dec peak, mid-year EOFY bump, quiet Feb."""
    doy = day.timetuple().tm_yday
    base = 1.0 + 0.12 * math.cos(2 * math.pi * (doy - 350) / 365.25)
    if day.month == 11:
        base *= 1.25
    if day.month == 12 and day.day <= 24:
        base *= 1.35
    if day.month == 6 and day.day >= 15:
        base *= 1.15
    bf = cat.black_friday(day.year)
    if bf - dt.timedelta(days=1) <= day <= bf + dt.timedelta(days=3):  # BF weekend -> Cyber Monday
        base *= 2.6
    return base
