"""Batch step 4: at-risk customers (churn early warning, A2 §2.1.2.3).

Two explainable signals per pseudonymised customer, as of the end of the last complete day:

* frequency drop: orders in the last 30 days vs. their average 30-day rate over the prior 90
  days. Flag if the rate fell by >= 50% and the customer was a regular (>= 3 orders in the
  prior 90 days, so one-off buyers are not flagged).
* negative voice: VADER-negative reviews in the last 90 days (the unstructured signal).

score = 100 * (0.6 * frequency_drop + 0.4 * negativity), where frequency_drop in [0, 1] and
negativity = clip(-avg_compound_90d, 0, 1). A customer is at risk if either flag fires.
The reason text spells out the numbers so marketing can see why someone is on the list.
"""
from __future__ import annotations

import datetime as dt
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import Config

# Tuned on the synthetic data with scripts/at_risk_grid.py (TEST_LOG T10): >= 3 prior orders
# and avg <= -0.3 flagged 994 customers at precision 0.36; this setting keeps the brief's
# 50% drop rule but needs a real buying habit and clearly negative reviews.
MIN_PRIOR_ORDERS = 5
DROP_THRESHOLD = 0.5
NEG_COMPOUND = -0.5


def run(spark: SparkSession, cfg: Config, as_of: dt.date) -> dict:
    t0 = time.time()
    d30, d120, d90 = (as_of - dt.timedelta(days=n) for n in (30, 120, 90))
    orders = spark.read.format("delta").load(cfg.path("silver", "orders")).where(F.col("event_date") <= F.lit(as_of))
    freq = (orders.groupBy("customer_pid").agg(
        F.sum((F.col("event_date") > F.lit(d30)).cast("int")).alias("orders_30d"),
        F.sum(((F.col("event_date") > F.lit(d120)) & (F.col("event_date") <= F.lit(d30))).cast("int")).alias("orders_prior_90d"),
        F.max("event_date").alias("last_order_date"),
        F.round(F.sum("total"), 2).alias("lifetime_value")))
    reviews = (spark.read.format("delta").load(cfg.path("silver", "reviews_scored"))
               .where((F.col("event_date") > F.lit(d90)) & (F.col("event_date") <= F.lit(as_of))))
    voice = reviews.groupBy("customer_pid").agg(
        F.count("*").alias("reviews_90d"),
        F.sum((F.col("label") == "neg").cast("int")).alias("neg_reviews_90d"),
        F.round(F.avg("compound"), 3).alias("avg_compound_90d"))

    df = (freq.join(voice, "customer_pid", "full")
          .fillna({"orders_30d": 0, "orders_prior_90d": 0, "reviews_90d": 0, "neg_reviews_90d": 0})
          .withColumn("prior_rate_30d", F.col("orders_prior_90d") / 3.0)
          .withColumn("freq_drop", F.when(F.col("orders_prior_90d") >= MIN_PRIOR_ORDERS,
                                          F.greatest(F.lit(0.0), 1 - F.col("orders_30d") / F.col("prior_rate_30d")))
                      .otherwise(F.lit(0.0)))
          .withColumn("negativity", F.greatest(F.lit(0.0), F.least(F.lit(1.0), -F.coalesce("avg_compound_90d", F.lit(0.0)))))
          .withColumn("freq_flag", F.col("freq_drop") >= DROP_THRESHOLD)
          .withColumn("voice_flag", (F.col("neg_reviews_90d") >= 1) & (F.coalesce("avg_compound_90d", F.lit(0.0)) <= NEG_COMPOUND))
          .withColumn("score", F.round(100 * (0.6 * F.col("freq_drop") + 0.4 * F.col("negativity")), 1))
          .where(F.col("freq_flag") | F.col("voice_flag"))
          .withColumn("reason", F.concat_ws("; ",
              F.when(F.col("freq_flag"), F.format_string("orders/30d fell %.0f%% (%d vs %.1f usual)",
                     100 * F.col("freq_drop"), F.col("orders_30d"), F.col("prior_rate_30d"))),
              F.when(F.col("voice_flag"), F.format_string("%d negative review(s), avg sentiment %.2f",
                     F.col("neg_reviews_90d"), F.col("avg_compound_90d")))))
          .withColumn("as_of", F.lit(as_of))
          .select("customer_pid", "score", "reason", "freq_flag", "voice_flag", "orders_30d", "orders_prior_90d",
                  "prior_rate_30d", "freq_drop", "reviews_90d", "neg_reviews_90d", "avg_compound_90d",
                  "last_order_date", "lifetime_value", "as_of"))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "at_risk_customers"))
    out = spark.read.format("delta").load(cfg.path("gold", "at_risk_customers"))

    pdf = out.orderBy(F.desc("score")).toPandas()
    docs = []
    for r in pdf.to_dict("records"):
        r["_id"] = r["customer_pid"]
        for k, v in list(r.items()):
            if hasattr(v, "item"):
                r[k] = v.item()
            elif isinstance(v, dt.date) and not isinstance(v, dt.datetime):
                r[k] = str(v)
            if r[k] != r[k]:  # NaN -> None
                r[k] = None
        docs.append(r)
    mongo.replace_all("at_risk_customers", docs)

    customers = orders.select("customer_pid").distinct().count()
    stats = {"as_of": str(as_of), "customers": customers, "at_risk": len(docs),
             "freq_flag": int(pdf.freq_flag.sum()) if len(pdf) else 0,
             "voice_flag": int(pdf.voice_flag.sum()) if len(pdf) else 0,
             "both": int((pdf.freq_flag & pdf.voice_flag).sum()) if len(pdf) else 0,
             "seconds": round(time.time() - t0, 1)}
    stats.update(evaluate_against_planted(list(pdf.customer_pid) if len(pdf) else [],
                                          set(pdf[pdf.freq_flag].customer_pid) if len(pdf) else set()))
    print(f"[at-risk] {stats['at_risk']:,} of {customers:,} customers flagged (freq {stats['freq_flag']}, voice "
          f"{stats['voice_flag']}, both {stats['both']}); vs planted churn cohort: precision {stats.get('precision')}, "
          f"recall {stats.get('recall')}, precision@100 {stats.get('precision_top100')} ({stats['seconds']} s)", flush=True)
    return stats


def evaluate_against_planted(ranked: list, freq_flagged: set) -> dict:
    """Synthetic-data check: the generator planted a churn cohort, so we can measure how many the
    rules find. `ranked` is the flagged list sorted by score (highest first). (No such ground
    truth exists in production; there, the flag is validated by whether customers actually lapse.)"""
    try:
        from src.common.config import get_config
        from src.generator.events import World
        cfg = get_config()
        planted = World(cfg.pseudo_salt, cfg.num_customers, cfg.random_seed).churners
    except Exception as exc:  # generator code unavailable -> skip
        return {"evaluation_error": str(exc)}
    flagged = set(ranked)
    tp = len(flagged & planted)
    top = ranked[:100]
    return {"planted_churners": len(planted), "true_positives": tp,
            "precision": round(tp / len(flagged), 3) if flagged else None,
            "recall": round(tp / len(planted), 3) if planted else None,
            "precision_top100": round(len(set(top) & planted) / len(top), 3) if top else None,
            "freq_rule_recall": round(len(freq_flagged & planted) / len(planted), 3) if planted else None}
