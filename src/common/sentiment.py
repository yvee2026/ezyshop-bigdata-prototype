"""VADER sentiment (Hutto & Gilbert, 2014), shared by the speed and batch layers.

VADER is a lexicon + rules model tuned for short, informal text. It needs no training data
and no GPU, runs at tens of thousands of sentences per second per core, and every score is
explainable (word valences plus rules for negation, intensifiers, "but", punctuation).
"""
from __future__ import annotations

import pandas as pd

POS_THRESHOLD = 0.05   # thresholds recommended by the VADER authors
NEG_THRESHOLD = -0.05

_analyzer = None


def analyzer():
    global _analyzer
    if _analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


def compound(text: str | None) -> float:
    if not text:
        return 0.0
    return float(analyzer().polarity_scores(text)["compound"])


def label(score: float) -> str:
    if score >= POS_THRESHOLD:
        return "pos"
    if score <= NEG_THRESHOLD:
        return "neg"
    return "neu"


def rating_label(rating: int) -> str:
    """Ground-truth proxy from stars: 4-5 pos, 3 neu, 1-2 neg."""
    return "pos" if rating >= 4 else "neg" if rating <= 2 else "neu"


def compound_series(texts: pd.Series) -> pd.Series:
    a = analyzer()
    return texts.fillna("").map(lambda t: float(a.polarity_scores(t)["compound"]) if t else 0.0)


def vader_udf():
    """pandas UDF: text -> compound score. Imported lazily so non-Spark code can use this module."""
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("double")
    def _vader(texts: pd.Series) -> pd.Series:
        return compound_series(texts)

    return _vader
