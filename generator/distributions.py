from __future__ import annotations

import math
import random
from datetime import datetime

_HOURLY_RATE: dict[int, float] = {
    0: 0.02,  1: 0.01,  2: 0.01,  3: 0.01,
    4: 0.02,  5: 0.05,  6: 0.10,  7: 0.20,
    8: 0.40,  9: 0.55, 10: 0.60, 11: 0.75,
   12: 1.00, 13: 0.90, 14: 0.70, 15: 0.60,
   16: 0.65, 17: 0.80, 18: 1.20, 19: 1.40,
   20: 1.30, 21: 1.00, 22: 0.60, 23: 0.30,
}

WEEKEND_MULTIPLIER = 1.4


def arrival_rate(dt: datetime) -> float:
    base = _HOURLY_RATE[dt.hour]
    if dt.weekday() >= 5:
        base *= WEEKEND_MULTIPLIER
    return base


def next_inter_arrival(dt: datetime) -> float:
    rate = arrival_rate(dt)
    if rate <= 0:
        return 60.0
    return random.expovariate(rate)


def build_sku_weights(sku_ids: list[str], alpha: float = 1.2) -> dict[str, float]:
    shuffled = sku_ids[:]
    rng = random.Random(42)
    rng.shuffle(shuffled)
    raw = [1.0 / math.pow(rank + 1, alpha) for rank in range(len(shuffled))]
    total = sum(raw)
    return {sku: w / total for sku, w in zip(shuffled, raw, strict=True)}


def sample_sku(weights: dict[str, float]) -> str:
    skus = list(weights.keys())
    probs = list(weights.values())
    return random.choices(skus, weights=probs, k=1)[0]


def sample_quantity() -> int:
    return random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.10, 0.06, 0.04], k=1)[0]