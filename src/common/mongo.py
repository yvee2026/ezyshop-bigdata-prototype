"""MongoDB helpers for the serving layer (pymongo, no Spark connector)."""
from __future__ import annotations

from typing import Iterable

from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne, UpdateOne

from src.common.config import Config, get_config

_client: MongoClient | None = None


def db(cfg: Config | None = None):
    global _client
    cfg = cfg or get_config()
    if _client is None:
        _client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=10000)
    return _client[cfg.mongo_db]


def upsert_set(collection: str, docs: Iterable[dict], key: str = "_id") -> int:
    """Idempotent bulk upsert: $set the full document on a deterministic _id.

    Re-applying the same batch (e.g. a replay after a crash) leaves the collection unchanged,
    unlike insert or $inc.
    """
    ops = [UpdateOne({key: d[key]}, {"$set": d}, upsert=True) for d in docs]
    if not ops:
        return 0
    res = db()[collection].bulk_write(ops, ordered=False)
    return res.upserted_count + res.modified_count


def replace_all(collection: str, docs: list[dict]) -> int:
    """Batch-layer outputs replace the previous night's results (also idempotent)."""
    coll = db()[collection]
    if not docs:
        return 0
    ids = [d["_id"] for d in docs]
    coll.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
    coll.delete_many({"_id": {"$nin": ids}})
    return len(docs)


def ensure_indexes() -> None:
    d = db()
    d.live_metrics.create_index([("window_start", DESCENDING)])
    d.live_metrics.create_index([("category", ASCENDING), ("window_start", DESCENDING)])
    d.live_sentiment.create_index([("window_start", DESCENDING)])
    d.pipeline_metrics.create_index([("ts", DESCENDING)])
    d.pipeline_metrics.create_index([("type", ASCENDING), ("ts", DESCENDING)])
    d.at_risk_customers.create_index([("score", DESCENDING)])
    d.review_sentiment.create_index([("category", ASCENDING)])
