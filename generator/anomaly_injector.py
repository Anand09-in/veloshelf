from __future__ import annotations

import json
import random
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path


class AnomalyType(StrEnum):
    SURGE = "surge"
    STOCKOUT_RISK = "stockout_risk"


@dataclass
class InjectionSignal:
    anomaly_type: AnomalyType
    store_id: str
    sku_id: str
    surge_extra_events: int = 0
    stockout_target_units: int = 5


@dataclass
class GroundTruthRecord:
    anomaly_type: str
    store_id: str
    sku_id: str
    injected_at: str
    surge_extra_events: int
    stockout_target_units: int


class AnomalyInjector:
    def __init__(
        self,
        sku_ids: list[str],
        store_ids: list[str],
        label_path: Path,
        surge_interval_s: float = 120.0,
        stockout_interval_s: float = 180.0,
        surge_extra_events: int = 20,
        stockout_target_units: int = 5,
        seed: int = 42,
    ) -> None:
        self._sku_ids = sku_ids
        self._store_ids = store_ids
        self._label_path = label_path
        self._surge_interval = surge_interval_s
        self._stockout_interval = stockout_interval_s
        self._surge_extra = surge_extra_events
        self._stockout_target = stockout_target_units
        self._rng = random.Random(seed)
        self._last_surge_t: float = time.monotonic()
        self._last_stockout_t: float = time.monotonic()
        label_path.parent.mkdir(parents=True, exist_ok=True)

    def check(self) -> InjectionSignal | None:
        now = time.monotonic()
        if now - self._last_surge_t >= self._surge_interval:
            self._last_surge_t = now
            return self._make_surge()
        if now - self._last_stockout_t >= self._stockout_interval:
            self._last_stockout_t = now
            return self._make_stockout()
        return None

    def _make_surge(self) -> InjectionSignal:
        signal = InjectionSignal(
            anomaly_type=AnomalyType.SURGE,
            store_id=self._rng.choice(self._store_ids),
            sku_id=self._rng.choice(self._sku_ids),
            surge_extra_events=self._surge_extra,
        )
        self._log(signal)
        return signal

    def _make_stockout(self) -> InjectionSignal:
        signal = InjectionSignal(
            anomaly_type=AnomalyType.STOCKOUT_RISK,
            store_id=self._rng.choice(self._store_ids),
            sku_id=self._rng.choice(self._sku_ids),
            stockout_target_units=self._stockout_target,
        )
        self._log(signal)
        return signal

    def _log(self, signal: InjectionSignal) -> None:
        record = GroundTruthRecord(
            anomaly_type=signal.anomaly_type,
            store_id=signal.store_id,
            sku_id=signal.sku_id,
            injected_at=datetime.now(tz=UTC).isoformat(),
            surge_extra_events=signal.surge_extra_events,
            stockout_target_units=signal.stockout_target_units,
        )
        with self._label_path.open("a") as f:
            f.write(json.dumps(asdict(record)) + "\n")
        print(f"[ANOMALY] {signal.anomaly_type.upper()} | "
              f"store={signal.store_id} sku={signal.sku_id}")