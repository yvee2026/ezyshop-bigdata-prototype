"""Forecast feature engineering: lags are correct and nothing leaks across the time split."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

from src.batch.forecast import bf_weeks, build_features, split

START = dt.date(2025, 1, 6)  # a Monday


@pytest.fixture(scope="module")
def weekly(spark):
    rows = []
    for c, base in (("a", 100.0), ("b", 1000.0)):
        for i in range(70):
            rows.append((c, START + dt.timedelta(weeks=i), base + i))  # units = base + week index
    return spark.createDataFrame(rows, "category string, week_start date, units double")


def test_lags_and_rolling_mean(weekly):
    f = build_features(weekly, None, None).where((F.col("category") == "a")).orderBy("week_start").collect()
    r = f[60]  # week index 60 -> units 160
    assert r.units == 160 and r.lag1 == 159 and r.lag2 == 158 and r.lag4 == 156 and r.lag52 == 108
    assert r.roll4 == pytest.approx((159 + 158 + 157 + 156) / 4)   # previous 4 weeks only
    assert f[10].lag52 is None and f[10].has52 == 0 and f[10].r52 == 1.0


def test_features_never_use_the_target_week(weekly):
    f = build_features(weekly, None, None)
    # if any feature used the current week, changing that week's units would change it
    bumped = weekly.withColumn("units", F.when(F.col("week_start") == START + dt.timedelta(weeks=60),
                                               F.col("units") + 10_000).otherwise(F.col("units")))
    g = build_features(bumped, None, None)
    cols = ["lag1", "lag2", "lag4", "lag52", "roll4", "r1", "r2", "r4", "r52", "sent_prev", "campaigns"]
    key = (F.col("week_start") == START + dt.timedelta(weeks=60))
    a = f.where(key).orderBy("category").select(cols).collect()
    b = g.where(key).orderBy("category").select(cols).collect()
    assert a == b


def test_lags_do_not_cross_categories(weekly):
    f = build_features(weekly, None, None).where(F.col("category") == "b").orderBy("week_start").first()
    assert f.lag1 is None  # first week of b must not see the last week of a


def test_time_split_has_no_overlap(weekly):
    f = build_features(weekly, None, None)
    train, test, test_weeks = split(f, test_weeks=8)
    assert len(test_weeks) == 8
    assert train.agg(F.max("week_start")).first()[0] < min(test_weeks)
    assert test.select("week_start").distinct().count() == 8
    assert train.where(F.col("n_prev") < 4).count() == 0  # needs 4 weeks of history


def test_black_friday_weeks_are_mondays():
    for w in bf_weeks():
        assert w.weekday() == 0
    assert dt.date(2025, 11, 24) in bf_weeks()  # BF 2025 = 28 Nov
