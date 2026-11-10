"""Batch step 2: sentiment on free-text reviews (the unstructured source).

VADER compound score through a pandas UDF (vectorised via Apache Arrow), labelled
pos / neu / neg with the authors' +/-0.05 thresholds. Outputs:

* silver/reviews_scored             one row per review with compound + label
* gold/review_sentiment             per product: n, avg compound, % pos/neg, avg stars
* gold/category_week_sentiment      per category x week (feeds the forecast + dashboard)
* validation: agreement between the VADER label and a star-rating label (4-5 pos, 3 neu,
  1-2 neg), plus a confusion matrix
"""
from __future__ import annotations

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import Config
from src.common.sentiment import NEG_THRESHOLD, POS_THRESHOLD, vader_udf


def run(spark: SparkSession, cfg: Config) -> dict:
    t0 = time.time()
    reviews = spark.read.format("delta").load(cfg.path("silver", "reviews"))
    scored = (reviews.withColumn("compound", vader_udf()(F.col("text")))
              .withColumn("label", F.when(F.col("compound") >= POS_THRESHOLD, "pos")
                          .when(F.col("compound") <= NEG_THRESHOLD, "neg").otherwise("neu"))
              .withColumn("rating_label", F.when(F.col("rating") >= 4, "pos")
                          .when(F.col("rating") <= 2, "neg").otherwise("neu"))
              .withColumn("week_start", F.date_trunc("week", "event_time").cast("date")))
    (scored.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .partitionBy("event_month").save(cfg.path("silver", "reviews_scored")))
    scored = spark.read.format("delta").load(cfg.path("silver", "reviews_scored"))

    per_product = (scored.groupBy("product_id", "category", "subcategory")
                   .agg(F.count("*").alias("reviews"), F.round(F.avg("compound"), 4).alias("avg_compound"),
                        F.round(F.avg("rating"), 3).alias("avg_rating"),
                        F.round(F.avg((F.col("label") == "pos").cast("double")), 4).alias("pct_pos"),
                        F.round(F.avg((F.col("label") == "neg").cast("double")), 4).alias("pct_neg")))
    per_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "review_sentiment"))

    cat_week = (scored.groupBy("category", "week_start")
                .agg(F.count("*").alias("reviews"), F.round(F.avg("compound"), 4).alias("avg_compound"),
                     F.round(F.avg("rating"), 3).alias("avg_rating")))
    cat_week.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "category_week_sentiment"))

    # ---- validation against star ratings
    total = scored.count()
    agree = scored.where(F.col("label") == F.col("rating_label")).count()
    conf = {f"{r['rating_label']}->{r['label']}": r["count"]
            for r in scored.groupBy("rating_label", "label").count().collect()}
    # polar-only agreement (ignores 3-star reviews, which are ambiguous by nature)
    polar = scored.where(F.col("rating_label") != "neu")
    polar_n = polar.count()
    polar_agree = polar.where(F.col("label") == F.col("rating_label")).count()
    corr = scored.stat.corr("compound", "rating")
    stats = {"reviews_scored": total, "agreement": round(agree / total, 4),
             "polar_agreement": round(polar_agree / polar_n, 4) if polar_n else None,
             "pearson_compound_vs_rating": round(corr, 4), "confusion": conf,
             "seconds": round(time.time() - t0, 1)}
    print(f"[sentiment] {total:,} reviews scored; agreement with stars {stats['agreement']:.1%} "
          f"(polar only {stats['polar_agreement']:.1%}), r={stats['pearson_compound_vs_rating']} ({stats['seconds']} s)",
          flush=True)
    return stats
