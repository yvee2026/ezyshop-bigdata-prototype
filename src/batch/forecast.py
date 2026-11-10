"""Batch step 3: weekly demand forecast per category (Spark MLlib), plus reorder signals.

Target: units sold per category x week (A2 Assumption 8: accuracy = 1 - MAPE at category x
week level). Pipeline:

1. gold/daily_category_sales  <- silver/order_items (units, revenue, orders per category/day)
2. weekly units per category, complete weeks only
3. features (build_features): lags 1/2/4/52, rolling mean of the previous 4 weeks, week of
   year, month, Black Friday flag, prior-week category sentiment, campaign count
4. time-based split: the last 8 weeks are the test set (no shuffling)
5. models: seasonal-naive (same week last year), LinearRegression, GBTRegressor.
   The ML models predict the ratio units / roll4 (scale-free across categories), so one model
   serves all 8 categories, and the ratio is turned back into units.
6. two evaluations:
   * one-step-ahead: each test week is predicted from actual history up to the previous week
   * recursive 8-week: predictions are fed back as lags (harder; closer to planning use)
7. best model retrained on all weeks -> next 4 weeks per category -> gold/forecast + Mongo
8. reorder signal: 4-week category forecast split to products by recent share, compared
   with Postgres inventory, supplier lead time and safety stock
"""
from __future__ import annotations

import datetime as dt
import time

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import Config
from src.generator.catalogue import black_friday

TEST_WEEKS = 8
HORIZON = 4
NUM_FEATURES = ["r1", "r2", "r4", "r52", "has52", "woy", "month", "bf_flag", "sent_prev", "campaigns"]


# ------------------------------------------------------------------------------ features
def bf_weeks(years=range(2024, 2029)) -> list[dt.date]:
    out = []
    for y in years:
        bf = black_friday(y)
        out.append(bf - dt.timedelta(days=bf.weekday()))  # Monday of the BF week
    return out


def weekly_campaigns(campaigns: DataFrame, categories: list[str]) -> DataFrame:
    """Number of campaigns active in each (category, week). Store-wide campaigns count for all."""
    days = campaigns.select("campaign_id", "category",
                            F.explode(F.sequence("start_date", "end_date")).alias("d"))
    weeks = days.select("campaign_id", "category", F.date_trunc("week", "d").cast("date").alias("week_start")).distinct()
    cats = weeks.sparkSession.createDataFrame([(c,) for c in categories], "cat string")
    expanded = weeks.join(cats, (weeks.category == cats.cat) | weeks.category.isNull())
    return (expanded.groupBy(F.col("cat").alias("category"), "week_start")
            .agg(F.countDistinct("campaign_id").alias("campaigns")))


def build_features(weekly: DataFrame, sentiment: DataFrame | None, campaigns: DataFrame | None) -> DataFrame:
    """weekly: (category, week_start, units). All features look strictly backwards in time.

    Lags are row-based, which is correct because every category has a row for every week
    (checked in run()); the rolling mean covers rows -4..-1 so it never contains the target week.
    """
    w = Window.partitionBy("category").orderBy("week_start")
    df = weekly
    if sentiment is not None:
        df = df.join(sentiment.select("category", "week_start", "avg_compound"), ["category", "week_start"], "left")
    else:
        df = df.withColumn("avg_compound", F.lit(None).cast("double"))
    if campaigns is not None:
        df = df.join(campaigns, ["category", "week_start"], "left")
    else:
        df = df.withColumn("campaigns", F.lit(0))
    df = (df.withColumn("lag1", F.lag("units", 1).over(w))
            .withColumn("lag2", F.lag("units", 2).over(w))
            .withColumn("lag4", F.lag("units", 4).over(w))
            .withColumn("lag52", F.lag("units", 52).over(w))
            .withColumn("roll4", F.avg("units").over(w.rowsBetween(-4, -1)))
            .withColumn("n_prev", F.count("units").over(w.rowsBetween(Window.unboundedPreceding, -1)))
            .withColumn("sent_prev", F.coalesce(F.lag("avg_compound", 1).over(w), F.lit(0.0)))
            .withColumn("campaigns", F.coalesce("campaigns", F.lit(0)).cast("double"))
            .withColumn("woy", F.weekofyear("week_start").cast("double"))
            .withColumn("month", F.month(F.date_add("week_start", 3)).cast("double"))
            .withColumn("bf_flag", F.col("week_start").isin(*bf_weeks()).cast("double"))
            .withColumn("has52", F.col("lag52").isNotNull().cast("double")))
    df = (df.withColumn("r1", F.col("lag1") / F.col("roll4"))
            .withColumn("r2", F.col("lag2") / F.col("roll4"))
            .withColumn("r4", F.col("lag4") / F.col("roll4"))
            .withColumn("r52", F.coalesce(F.col("lag52") / F.col("roll4"), F.lit(1.0)))
            .withColumn("label", F.col("units") / F.col("roll4")))
    return df


def split(features: DataFrame, test_weeks: int = TEST_WEEKS) -> tuple[DataFrame, DataFrame, list]:
    weeks = sorted(r.week_start for r in features.select("week_start").distinct().collect())
    test_set = weeks[-test_weeks:]
    usable = features.where(F.col("n_prev") >= 4)
    train = usable.where(~F.col("week_start").isin(*test_set))
    test = usable.where(F.col("week_start").isin(*test_set))
    return train, test, test_set


# ------------------------------------------------------------------------------ models
def make_pipelines() -> dict[str, Pipeline]:
    idx = StringIndexer(inputCol="category", outputCol="cat_idx", handleInvalid="keep")
    ohe = OneHotEncoder(inputCols=["cat_idx"], outputCols=["cat_ohe"])
    lr_vec = VectorAssembler(inputCols=NUM_FEATURES + ["cat_ohe"], outputCol="features")
    gbt_vec = VectorAssembler(inputCols=NUM_FEATURES + ["cat_idx"], outputCol="features")
    return {
        "linear_regression": Pipeline(stages=[idx, ohe, lr_vec, LinearRegression(regParam=0.01, elasticNetParam=0.0)]),
        "gbt": Pipeline(stages=[idx, gbt_vec, GBTRegressor(maxIter=120, maxDepth=4, stepSize=0.08, seed=42)]),
    }


def mape(df: DataFrame, pred_col: str) -> tuple[float, dict]:
    e = df.withColumn("ape", F.abs(F.col("units") - F.col(pred_col)) / F.col("units"))
    overall = e.agg(F.avg("ape")).first()[0]
    per_cat = {r.category: r["m"] for r in e.groupBy("category").agg(F.avg("ape").alias("m")).collect()}
    return overall, per_cat


def recursive_forecast(spark: SparkSession, model, weekly_hist: DataFrame, sentiment, campaigns,
                       future_weeks: list[dt.date]) -> DataFrame:
    """Predict week by week, appending each prediction as 'units' so later lags use it.
    Re-uses build_features, so training and forecasting share one feature definition."""
    hist = weekly_hist.select("category", "week_start", F.col("units").cast("double"))
    categories = [r.category for r in hist.select("category").distinct().collect()]
    preds = []
    for wk in future_weeks:
        placeholder = spark.createDataFrame([(c, wk, None) for c in categories], "category string, week_start date, units double")
        feats = build_features(hist.unionByName(placeholder), sentiment, campaigns).where(F.col("week_start") == F.lit(wk))
        out = model.transform(feats.drop("label")).select(
            "category", "week_start", (F.col("prediction") * F.col("roll4")).alias("units"))
        rows = out.collect()
        preds += rows
        hist = hist.unionByName(spark.createDataFrame(rows, "category string, week_start date, units double"))
    return spark.createDataFrame(preds, "category string, week_start date, units double")


# ------------------------------------------------------------------------------ main step
def run(spark: SparkSession, cfg: Config, as_of: dt.date) -> dict:
    t0 = time.time()
    items = spark.read.format("delta").load(cfg.path("silver", "order_items")).where(F.col("event_date") <= F.lit(as_of))

    daily = (items.groupBy("category", "event_date")
             .agg(F.sum("qty").alias("units"), F.round(F.sum("line_revenue"), 2).alias("revenue"),
                  F.countDistinct("order_id").alias("orders")))
    daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "daily_category_sales"))

    first_day = daily.agg(F.min("event_date")).first()[0]
    first_monday = first_day + dt.timedelta(days=(7 - first_day.weekday()) % 7)
    last_full_week = as_of - dt.timedelta(days=(as_of.weekday() + 1) % 7) - dt.timedelta(days=6)  # Monday of last full week
    weekly = (spark.read.format("delta").load(cfg.path("gold", "daily_category_sales"))
              .withColumn("week_start", F.date_trunc("week", "event_date").cast("date"))
              .where((F.col("week_start") >= F.lit(first_monday)) & (F.col("week_start") <= F.lit(last_full_week)))
              .groupBy("category", "week_start").agg(F.sum("units").cast("double").alias("units")))
    weekly = weekly.where(F.col("category") != "unknown").cache()

    n_weeks = weekly.select("week_start").distinct().count()
    categories = sorted(r.category for r in weekly.select("category").distinct().collect())
    gaps = weekly.groupBy("category").count().where(F.col("count") != n_weeks).count()
    if gaps:
        raise RuntimeError("missing category-weeks: row-based lags would be wrong")

    sentiment = spark.read.format("delta").load(cfg.path("gold", "category_week_sentiment"))
    camp = weekly_campaigns(spark.read.format("delta").load(cfg.path("silver", "campaigns")), categories).cache()
    feats = build_features(weekly, sentiment, camp).cache()
    feats.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "category_week_features"))

    train, test, test_weeks = split(feats)
    results: dict = {"weeks": n_weeks, "train_rows": train.count(), "test_rows": test.count(),
                     "test_weeks": [str(w) for w in test_weeks], "as_of": str(as_of)}

    # ---- one-step-ahead evaluation
    evals = test.withColumn("seasonal_naive", F.col("lag52"))
    fitted = {}
    for name, pipe in make_pipelines().items():
        m = pipe.fit(train)
        fitted[name] = m
        evals = evals.join(m.transform(test).select("category", "week_start", (F.col("prediction") * F.col("roll4")).alias(name)),
                           ["category", "week_start"])
    evals = evals.cache()
    metrics = {}
    for name in ["seasonal_naive", "linear_regression", "gbt"]:
        overall, per_cat = mape(evals.where(F.col(name).isNotNull()), name)
        metrics[name] = {"mape": round(overall, 4), "accuracy": round(1 - overall, 4),
                         "per_category_accuracy": {k: round(1 - v, 4) for k, v in sorted(per_cat.items())}}
    best = min(["linear_regression", "gbt"], key=lambda n: metrics[n]["mape"])
    results["one_step"] = metrics
    results["best_model"] = best

    # ---- recursive 8-week evaluation of the best model (trained on train only)
    hist = weekly.where(F.col("week_start") < F.lit(test_weeks[0]))
    rec = recursive_forecast(spark, fitted[best], hist, sentiment, camp, test_weeks).withColumnRenamed("units", "recursive")
    rec_eval = evals.select("category", "week_start", "units").join(rec, ["category", "week_start"])
    r_overall, r_cat = mape(rec_eval, "recursive")
    results["recursive_8w"] = {"model": best, "mape": round(r_overall, 4), "accuracy": round(1 - r_overall, 4),
                               "per_category_accuracy": {k: round(1 - v, 4) for k, v in sorted(r_cat.items())}}

    (evals.select("category", "week_start", "units", "seasonal_naive", "linear_regression", "gbt")
     .join(rec, ["category", "week_start"], "left")
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "forecast_eval")))

    # ---- final model on all weeks -> next 4 weeks
    final = make_pipelines()[best].fit(feats.where(F.col("n_prev") >= 4))
    future_weeks = [last_full_week + dt.timedelta(weeks=k) for k in range(1, HORIZON + 1)]
    fut = recursive_forecast(spark, final, weekly, sentiment, camp, future_weeks)
    fut = fut.withColumn("model", F.lit(best)).withColumn("forecast_units", F.round("units", 0)).drop("units")
    fut.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "forecast"))
    results["future_weeks"] = [str(w) for w in future_weeks]

    # ---- reorder signal vs Postgres inventory (silver snapshot)
    reorder = reorder_signals(spark, cfg, items, fut, last_full_week)
    results["reorder"] = {"products": reorder.count(),
                          "reorder_now": reorder.where("status='REORDER'").count(),
                          "overstock": reorder.where("status='OVERSTOCK'").count()}

    publish(spark, cfg, weekly, evals, rec, fut, reorder, results)
    results["seconds"] = round(time.time() - t0, 1)
    print(f"[forecast] weeks={n_weeks} train={results['train_rows']} test={results['test_rows']} | one-step accuracy: "
          + ", ".join(f"{k}={v['accuracy']:.1%}" for k, v in metrics.items())
          + f" | recursive 8w ({best}) {results['recursive_8w']['accuracy']:.1%} | reorder "
          f"{results['reorder']['reorder_now']}/{results['reorder']['products']} ({results['seconds']} s)", flush=True)
    return results


def reorder_signals(spark: SparkSession, cfg: Config, items: DataFrame, fut: DataFrame, last_full_week: dt.date) -> DataFrame:
    recent = items.where(F.col("event_date") >= F.lit(last_full_week - dt.timedelta(weeks=7)))
    prod_units = recent.groupBy("product_id", "category").agg(F.sum("qty").alias("p_units"))
    cat_units = prod_units.groupBy("category").agg(F.sum("p_units").alias("c_units"))
    share = prod_units.join(cat_units, "category").withColumn("share", F.col("p_units") / F.col("c_units"))
    cat_fc = fut.groupBy("category").agg(F.sum("forecast_units").alias("cat_forecast_4w"))
    inv = spark.read.format("delta").load(cfg.path("silver", "inventory"))
    sup = spark.read.format("delta").load(cfg.path("silver", "suppliers")).select("supplier_id", "lead_time_days")
    prods = spark.read.format("delta").load(cfg.path("silver", "products")).select("product_id", "name", "supplier_id")
    df = (share.join(cat_fc, "category").join(inv, "product_id").join(prods, "product_id").join(sup, "supplier_id")
          .withColumn("demand_4w", F.round(F.col("share") * F.col("cat_forecast_4w"), 0))
          .withColumn("daily_demand", F.col("demand_4w") / 28)
          .withColumn("lead_time_demand", F.round(F.col("daily_demand") * F.col("lead_time_days"), 0))
          .withColumn("reorder_level", F.col("lead_time_demand") + F.col("safety_stock"))
          .withColumn("days_of_cover", F.round(F.col("on_hand") / F.greatest(F.col("daily_demand"), F.lit(0.01)), 1))
          .withColumn("status", F.when(F.col("on_hand") < F.col("reorder_level"), "REORDER")
                      .when(F.col("on_hand") > 3 * F.col("demand_4w"), "OVERSTOCK").otherwise("OK"))
          .withColumn("suggested_qty", F.when(F.col("status") == "REORDER",
                      F.col("demand_4w") + F.col("reorder_level") - F.col("on_hand")).otherwise(F.lit(0)).cast("int"))
          .select("product_id", "name", "category", "warehouse", "on_hand", "safety_stock", "lead_time_days",
                  "demand_4w", "lead_time_demand", "reorder_level", "days_of_cover", "status", "suggested_qty"))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cfg.path("gold", "reorder_signals"))
    return spark.read.format("delta").load(cfg.path("gold", "reorder_signals"))


def publish(spark, cfg, weekly, evals, rec, fut, reorder, results) -> None:
    hist = weekly.toPandas()
    ev = evals.select("category", "week_start", "units", "seasonal_naive", "linear_regression", "gbt").toPandas()
    rc = rec.toPandas()
    fu = fut.toPandas()
    docs = []
    for c in sorted(hist.category.unique()):
        h = hist[hist.category == c].sort_values("week_start")
        e = ev[ev.category == c].sort_values("week_start").merge(rc[rc.category == c][["week_start", "recursive"]],
                                                                  on="week_start", how="left")
        f = fu[fu.category == c].sort_values("week_start")
        docs.append({
            "_id": c, "category": c, "model": results["best_model"],
            "history": [{"week_start": str(r.week_start), "units": float(r.units)} for r in h.itertuples()],
            "test": [{"week_start": str(r.week_start), "actual": float(r.units),
                      "seasonal_naive": None if r.seasonal_naive != r.seasonal_naive else float(r.seasonal_naive),
                      "linear_regression": float(r.linear_regression), "gbt": float(r.gbt),
                      "recursive": None if r.recursive != r.recursive else float(r.recursive)} for r in e.itertuples()],
            "future": [{"week_start": str(r.week_start), "forecast_units": float(r.forecast_units)} for r in f.itertuples()],
            "accuracy": {m: results["one_step"][m]["per_category_accuracy"].get(c) for m in results["one_step"]},
            "recursive_accuracy": results["recursive_8w"]["per_category_accuracy"].get(c),
        })
    docs.append({"_id": "__metrics__", **{k: v for k, v in results.items() if k != "reorder"},
                 "target_accuracy": 0.90, "updated_at": dt.datetime.now(dt.timezone.utc)})
    mongo.replace_all("forecasts", docs)
    alerts = [dict(r, _id=r["product_id"]) for r in reorder.toPandas().to_dict("records")]
    for a in alerts:
        for k, v in a.items():
            if hasattr(v, "item"):
                a[k] = v.item()
    mongo.replace_all("reorder_alerts", alerts)
