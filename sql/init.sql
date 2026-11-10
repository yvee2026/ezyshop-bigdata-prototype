-- EzyShop system of record (PostgreSQL 16).
-- Schema only; rows are seeded deterministically by `python -m src.generator.catalogue --seed-db`
-- (run by `make init`) so the generator and the database always agree on product IDs and prices.

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     TEXT PRIMARY KEY,
    name            TEXT        NOT NULL,
    country         TEXT        NOT NULL,
    lead_time_days  INTEGER     NOT NULL CHECK (lead_time_days > 0),
    reliability     NUMERIC(4,3) NOT NULL CHECK (reliability BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS products (
    product_id   TEXT PRIMARY KEY,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    subcategory  TEXT          NOT NULL,
    price        NUMERIC(10,2) NOT NULL CHECK (price > 0),
    supplier_id  TEXT          NOT NULL REFERENCES suppliers(supplier_id)
);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

CREATE TABLE IF NOT EXISTS inventory (
    product_id     TEXT PRIMARY KEY REFERENCES products(product_id),
    warehouse      TEXT        NOT NULL,
    on_hand        INTEGER     NOT NULL CHECK (on_hand >= 0),
    reorder_point  INTEGER     NOT NULL,
    safety_stock   INTEGER     NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Campaigns are the fifth silo named in the case study (A2 Figure 1).
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id   TEXT PRIMARY KEY,
    name          TEXT          NOT NULL,
    channel       TEXT          NOT NULL,
    category      TEXT,                       -- NULL = store-wide
    start_date    DATE          NOT NULL,
    end_date      DATE          NOT NULL,
    discount_pct  NUMERIC(5,2)  NOT NULL,
    CHECK (end_date >= start_date)
);

-- Orders stay relational (A2 §2.2.1: ACID system of record). Used by the order-write benchmark.
CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID PRIMARY KEY,
    customer_pid    CHAR(64)      NOT NULL,
    event_time      TIMESTAMPTZ   NOT NULL,
    total           NUMERIC(12,2) NOT NULL,
    payment_method  TEXT          NOT NULL,
    campaign_id     TEXT
);
