import pytest

from src.common.sentiment import compound, label, rating_label


@pytest.mark.parametrize("text,expected", [
    ("Excellent quality, I love it. Highly recommend!", "pos"),
    ("Terrible. It broke after a week and support was useless.", "neg"),
    ("It arrived on Tuesday.", "neu"),
    ("The battery is not good.", "neg"),            # negation handled by VADER rules
])
def test_known_sentences(text, expected):
    assert label(compound(text)) == expected


def test_known_failure_sarcasm_is_documented():
    # Lexicon models read sarcasm literally: this 1-star complaint scores positive.
    # Kept as a test so the limitation stays visible (README: limitations; TEST_LOG).
    s = compound("Great, the battery broke after two days. Just what I wanted.")
    assert label(s) == "pos"


def test_empty_text_is_neutral():
    assert compound("") == 0.0 and compound(None) == 0.0 and label(0.0) == "neu"


def test_rating_label():
    assert [rating_label(r) for r in (1, 2, 3, 4, 5)] == ["neg", "neg", "neu", "pos", "pos"]
