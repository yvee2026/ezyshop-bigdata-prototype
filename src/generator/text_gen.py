"""Synthetic free-text product reviews (the unstructured source).

Text sentiment is correlated with the star rating but deliberately noisy:
* ~7% sarcastic reviews: low rating written with positive words ("Great, broke in two days").
* ~12% mixed reviews: one strong positive and one strong negative clause.
* length varies from one short sentence to a paragraph, with Faker filler for randomness.

These are the cases a lexicon model like VADER is known to get wrong, so the sentiment
validation step (rating vs. VADER label agreement) has realistic errors to report.
"""
from __future__ import annotations

import random

from faker import Faker

ASPECTS = {
    "electronics": ["sound", "battery", "picture", "build quality", "setup"],
    "computing": ["performance", "screen", "keyboard", "battery", "build quality"],
    "phones": ["battery", "camera", "charging speed", "fit", "build quality"],
    "apparel": ["fit", "fabric", "colour", "stitching", "sizing"],
    "footwear": ["comfort", "fit", "grip", "sizing", "durability"],
    "home": ["design", "size", "colour", "durability", "assembly"],
    "kitchen": ["performance", "cleaning", "build quality", "noise", "size"],
    "beauty": ["scent", "texture", "packaging", "results", "value"],
}

POS = ["excellent", "fantastic", "really good", "great", "perfect", "lovely", "impressive", "superb"]
NEG = ["terrible", "awful", "really poor", "disappointing", "useless", "flimsy", "horrible", "bad"]
NEU = ["okay", "average", "fine", "as expected", "acceptable", "standard"]

POS_SENT = [
    "The {a} is {p}.", "I love the {a}, {p} value for money.", "Delivery was quick and the {a} is {p}.",
    "Would definitely recommend, the {a} is {p}.", "Very happy with this purchase.",
    "Five stars, the {a} exceeded my expectations.", "Bought a second one for my partner.",
]
NEG_SENT = [
    "The {a} is {n}.", "Really disappointed with the {a}.", "The {a} stopped working after a week.",
    "Would not recommend, the {a} is {n}.", "Returned it, the {a} was {n}.",
    "Customer service never replied about the {a}.", "Waste of money, the {a} is {n}.",
]
NEU_SENT = [
    "The {a} is {u}.", "It does the job.", "Nothing special about the {a}.",
    "Arrived on time, the {a} is {u}.", "It is {u} for the price.",
]
SARCASM = [
    "Great, the {a} broke after two days. Just what I wanted.",
    "Oh wonderful, another {a} that falls apart. Love it.",
    "Fantastic, the {a} lasted almost a whole week. Amazing quality.",
    "Brilliant, the {a} is exactly as useless as the reviews warned.",
    "Perfect if you enjoy returning things. The {a} is a joke.",
]
MIXED = [
    "I love the {a1} but the {a2} is {n}.",
    "The {a1} is {p}, however the {a2} is {n}.",
    "Terrible {a2}, but honestly the {a1} is {p}.",
    "Great {a1}, shame about the {a2}, it is {n}.",
]
TITLES = {
    5: ["Love it", "Excellent", "Highly recommend", "Best purchase this year"],
    4: ["Very good", "Happy overall", "Good value", "Solid choice"],
    3: ["It's okay", "Average", "Does the job", "Mixed feelings"],
    2: ["Disappointed", "Not great", "Expected more", "Meh"],
    1: ["Avoid", "Terrible", "Waste of money", "Do not buy"],
}

_fake = Faker()


def review_text(rating: int, category: str, rng: random.Random) -> tuple[str, str, str]:
    """Return (title, text, kind) where kind is normal|sarcastic|mixed (kept only for testing)."""
    aspects = ASPECTS.get(category, ["quality", "value"])
    a = rng.choice(aspects)
    r = rng.random()
    if rating <= 2 and r < 0.18:  # ~7% overall: sarcasm only makes sense on low ratings
        text = rng.choice(SARCASM).format(a=a)
        return rng.choice(TITLES[rating]), text, "sarcastic"
    if r > 0.88:
        a1, a2 = rng.sample(aspects, 2) if len(aspects) > 1 else (a, a)
        text = rng.choice(MIXED).format(a1=a1, a2=a2, p=rng.choice(POS), n=rng.choice(NEG))
        return rng.choice(TITLES[rating]), text, "mixed"

    pool = POS_SENT if rating >= 4 else NEG_SENT if rating <= 2 else NEU_SENT
    n_sent = rng.choices([1, 2, 3, 4, 6], weights=[30, 30, 20, 12, 8])[0]
    sents = []
    for _ in range(n_sent):
        a = rng.choice(aspects)
        sents.append(rng.choice(pool).format(a=a, p=rng.choice(POS), n=rng.choice(NEG), u=rng.choice(NEU)))
    if rng.random() < 0.25:  # neutral filler for length/variety
        _fake.seed_instance(rng.randint(0, 10**9))
        sents.insert(rng.randint(0, len(sents)), _fake.sentence(nb_words=rng.randint(5, 12)))
    return rng.choice(TITLES[rating]), " ".join(sents), "normal"


def rating_from_mood(mood: float, rng: random.Random) -> int:
    """mood in [-1, 1] -> star rating 1..5 with noise."""
    x = 3 + 2.2 * mood + rng.gauss(0, 0.9)
    return int(min(5, max(1, round(x))))
