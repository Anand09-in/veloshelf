"""Phase 1 unit tests — generator logic.

Tests cover distributions, inventory state, anomaly injector, and schemas.
These do NOT require Kafka to be running.
"""

from __future__ import annotations

import json
import math
import time
from datetime import UTC

import pytest

from generator.anomaly_injector import AnomalyInjector, AnomalyType
from generator.distributions import (
    arrival_rate,
    build_sku_weights,
    next_inter_arrival,
    sample_quantity,
    sample_sku,
)
from generator.inventory import InventoryState
from generator.schemas import DimSku, DimStore, MovementType, OrderEvent

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def skus() -> list[DimSku]:
    return [
        DimSku(sku_id="SKU_001", name="Milk", category="dairy",
               unit_price=27.0, reorder_point=40),
        DimSku(sku_id="SKU_002", name="Bread", category="bakery",
               unit_price=45.0, reorder_point=25),
        DimSku(sku_id="SKU_003", name="Eggs", category="dairy",
               unit_price=54.0, reorder_point=30),
    ]


@pytest.fixture
def stores() -> list[DimStore]:
    return [
        DimStore(store_id="DS_001", region="Bengaluru-Koramangala", capacity=2000),
        DimStore(store_id="DS_002", region="Bengaluru-Indiranagar", capacity=1800),
    ]


# ---------------------------------------------------------------------------
# distributions
# ---------------------------------------------------------------------------

class TestArrivalRate:
    def test_peak_hour_higher_than_overnight(self):
        from datetime import datetime
        peak = datetime(2024, 6, 3, 19)      # Monday 7pm
        night = datetime(2024, 6, 3, 2)      # Monday 2am
        assert arrival_rate(peak) > arrival_rate(night)

    def test_weekend_higher_than_weekday(self):
        from datetime import datetime
        saturday = datetime(2024, 6, 8, 12)  # Saturday noon
        monday = datetime(2024, 6, 3, 12)    # Monday noon
        assert arrival_rate(saturday) > arrival_rate(monday)

    def test_inter_arrival_positive(self):
        from datetime import datetime
        dt = datetime(2024, 6, 3, 12)
        gap = next_inter_arrival(dt)
        assert gap > 0


class TestSkuWeights:
    def test_weights_sum_to_one(self, skus):
        sku_ids = [s.sku_id for s in skus]
        weights = build_sku_weights(sku_ids)
        assert math.isclose(sum(weights.values()), 1.0, rel_tol=1e-9)

    def test_all_skus_represented(self, skus):
        sku_ids = [s.sku_id for s in skus]
        weights = build_sku_weights(sku_ids)
        assert set(weights.keys()) == set(sku_ids)

    def test_sample_returns_valid_sku(self, skus):
        sku_ids = [s.sku_id for s in skus]
        weights = build_sku_weights(sku_ids)
        result = sample_sku(weights)
        assert result in sku_ids

    def test_quantity_in_expected_range(self):
        for _ in range(100):
            assert 1 <= sample_quantity() <= 5


# ---------------------------------------------------------------------------
# inventory
# ---------------------------------------------------------------------------

class TestInventoryState:
    def test_initial_stock_positive(self, stores, skus):
        inv = InventoryState.initialise(stores, skus)
        for store in stores:
            for sku in skus:
                assert inv.on_hand(store.store_id, sku.sku_id) > 0

    def test_sell_decrements_stock(self, stores, skus):
        inv = InventoryState.initialise(stores, skus)
        before = inv.on_hand("DS_001", "SKU_001")
        result = inv.sell("DS_001", "SKU_001", 2)
        assert result == before - 2
        assert inv.on_hand("DS_001", "SKU_001") == before - 2

    def test_sell_returns_none_when_insufficient(self, stores, skus):
        inv = InventoryState.initialise(stores, skus)
        inv.stock[("DS_001", "SKU_001")] = 1
        result = inv.sell("DS_001", "SKU_001", 5)
        assert result is None
        assert inv.on_hand("DS_001", "SKU_001") == 1  # unchanged

    def test_restock_increases_stock(self, stores, skus):
        inv = InventoryState.initialise(stores, skus)
        inv.stock[("DS_001", "SKU_001")] = 0
        new_level = inv.restock("DS_001", "SKU_001")
        assert new_level > 0

    def test_force_deplete_reduces_stock(self, stores, skus):
        inv = InventoryState.initialise(stores, skus)
        inv.stock[("DS_001", "SKU_001")] = 100
        new_level = inv.force_deplete("DS_001", "SKU_001", target=5)
        assert new_level == 5


# ---------------------------------------------------------------------------
# anomaly injector
# ---------------------------------------------------------------------------

class TestAnomalyInjector:
    def test_no_signal_before_interval(self, tmp_path, skus, stores):
        injector = AnomalyInjector(
            sku_ids=[s.sku_id for s in skus],
            store_ids=[s.store_id for s in stores],
            label_path=tmp_path / "labels.jsonl",
            surge_interval_s=9999,
            stockout_interval_s=9999,
        )
        assert injector.check() is None

    def test_surge_fires_after_interval(self, tmp_path, skus, stores):
        injector = AnomalyInjector(
            sku_ids=[s.sku_id for s in skus],
            store_ids=[s.store_id for s in stores],
            label_path=tmp_path / "labels.jsonl",
            surge_interval_s=0.01,
            stockout_interval_s=9999,
        )
        time.sleep(0.02)
        signal = injector.check()
        assert signal is not None
        assert signal.anomaly_type == AnomalyType.SURGE

    def test_ground_truth_written_to_jsonl(self, tmp_path, skus, stores):
        label_path = tmp_path / "labels.jsonl"
        injector = AnomalyInjector(
            sku_ids=[s.sku_id for s in skus],
            store_ids=[s.store_id for s in stores],
            label_path=label_path,
            surge_interval_s=0.01,
            stockout_interval_s=9999,
        )
        time.sleep(0.02)
        injector.check()
        assert label_path.exists()
        records = [json.loads(line) for line in label_path.read_text().splitlines()]
        assert len(records) == 1
        assert records[0]["anomaly_type"] == "surge"
        assert "injected_at" in records[0]

    def test_signal_sku_and_store_valid(self, tmp_path, skus, stores):
        injector = AnomalyInjector(
            sku_ids=[s.sku_id for s in skus],
            store_ids=[s.store_id for s in stores],
            label_path=tmp_path / "labels.jsonl",
            surge_interval_s=0.01,
            stockout_interval_s=9999,
        )
        time.sleep(0.02)
        signal = injector.check()
        assert signal.sku_id in [s.sku_id for s in skus]
        assert signal.store_id in [s.store_id for s in stores]


# ---------------------------------------------------------------------------
# schemas
# ---------------------------------------------------------------------------

class TestSchemas:
    def test_order_event_rejects_zero_quantity(self):
        from datetime import datetime

        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            OrderEvent(
                event_id="x", event_time=datetime.now(tz=UTC),
                store_id="DS_001", sku_id="SKU_001", category="dairy",
                quantity=0, unit_price=27.0, order_id="o1",
            )

    def test_movement_type_values(self):
        assert MovementType.SALE == "sale"
        assert MovementType.RESTOCK == "restock"