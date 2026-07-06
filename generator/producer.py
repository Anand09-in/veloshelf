"""VeloShelf synthetic event generator — main entry point.

Simulates a Blinkit/Zepto-style dark store producing live order and inventory
events. Publishes to Kafka topics `raw-orders` and `raw-inventory`.

Usage:
    python -m generator.producer --mode realtime   # default: sleeps between events
    python -m generator.producer --mode fast       # no sleep: floods Kafka for testing
    python -m generator.producer --help
"""

from __future__ import annotations

import argparse
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from generator.anomaly_injector import AnomalyInjector, AnomalyType
from generator.distributions import (
    build_sku_weights,
    next_inter_arrival,
    sample_quantity,
    sample_sku,
)
from generator.inventory import InventoryState
from generator.kafka_client import kafka_producer
from generator.schemas import InventoryEvent, MovementType, OrderEvent
from generator.seed_loader import load_skus, load_stores

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
logger = logging.getLogger("veloshelf.generator")

# ---------------------------------------------------------------------------
# Config from environment (with sensible defaults for local dev)
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS = os.getenv("TOPIC_RAW_ORDERS", "raw-orders")
TOPIC_INVENTORY = os.getenv("TOPIC_RAW_INVENTORY", "raw-inventory")
LABEL_PATH = Path(os.getenv("SEED_DIR", "data")) / "anomaly_labels.jsonl"

# Anomaly schedule
SURGE_INTERVAL_S = float(os.getenv("SURGE_INTERVAL_S", "120"))
STOCKOUT_INTERVAL_S = float(os.getenv("STOCKOUT_INTERVAL_S", "180"))


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------

def make_order_event(
    store_id: str,
    sku_id: str,
    category: str,
    unit_price: float,
    quantity: int,
    is_anomaly: bool = False,
) -> OrderEvent:
    return OrderEvent(
        event_id=str(uuid.uuid4()),
        event_time=datetime.now(tz=timezone.utc),
        store_id=store_id,
        sku_id=sku_id,
        category=category,
        quantity=quantity,
        unit_price=unit_price,
        order_id=str(uuid.uuid4()),
        is_injected_anomaly=is_anomaly,
    )


def make_inventory_event(
    store_id: str,
    sku_id: str,
    quantity: int,
    on_hand_after: int,
    is_anomaly: bool = False,
) -> InventoryEvent:
    return InventoryEvent(
        event_id=str(uuid.uuid4()),
        event_time=datetime.now(tz=timezone.utc),
        store_id=store_id,
        sku_id=sku_id,
        movement_type=MovementType.SALE,
        delta_units=-quantity,
        on_hand_after=on_hand_after,
        is_injected_anomaly=is_anomaly,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(mode: str) -> None:
    logger.info("VeloShelf generator starting | mode=%s", mode)

    # Load seed dimensions
    skus = load_skus()
    stores = load_stores()
    sku_map = {s.sku_id: s for s in skus}
    store_ids = [s.store_id for s in stores]
    sku_ids = [s.sku_id for s in skus]

    # Build SKU popularity weights (Zipf)
    sku_weights = build_sku_weights(sku_ids)

    # Initialise inventory state
    inventory = InventoryState.initialise(stores, skus)

    # Anomaly injector
    injector = AnomalyInjector(
        sku_ids=sku_ids,
        store_ids=store_ids,
        label_path=LABEL_PATH,
        surge_interval_s=SURGE_INTERVAL_S,
        stockout_interval_s=STOCKOUT_INTERVAL_S,
    )

    events_sent = 0

    with kafka_producer(BOOTSTRAP_SERVERS) as producer:
        logger.info(
            "Connected to Kafka | brokers=%s | topics=%s, %s",
            BOOTSTRAP_SERVERS, TOPIC_ORDERS, TOPIC_INVENTORY,
        )

        while True:
            now = datetime.now(tz=timezone.utc)

            # --- Check for scheduled anomaly ---
            signal = injector.check()

            if signal and signal.anomaly_type == AnomalyType.STOCKOUT_RISK:
                # Force stock depletion; the following normal events will then
                # push this SKU to near-zero, triggering the detector.
                new_level = inventory.force_deplete(
                    signal.store_id,
                    signal.sku_id,
                    target=signal.stockout_target_units,
                )
                logger.info(
                    "[STOCKOUT_RISK] store=%s sku=%s stock→%d",
                    signal.store_id, signal.sku_id, new_level,
                )

            # --- Determine how many events to emit this tick ---
            # Normal: 1 event. Surge: 1 + extra burst events on the target SKU.
            burst_events: list[tuple[str, str, bool]] = []  # (store_id, sku_id, is_anomaly)

            if signal and signal.anomaly_type == AnomalyType.SURGE:
                # Inject a burst of extra orders for the target SKU
                for _ in range(signal.surge_extra_events):
                    burst_events.append((signal.store_id, signal.sku_id, True))

            # Always add one normal event
            store_id = store_ids[events_sent % len(store_ids)]   # round-robin stores
            sku_id = sample_sku(sku_weights)
            burst_events.append((store_id, sku_id, False))

            # --- Emit events ---
            for s_id, k_id, is_anomaly in burst_events:
                sku = sku_map[k_id]
                qty = sample_quantity()

                on_hand = inventory.sell(s_id, k_id, qty)
                if on_hand is None:
                    # Out of stock — trigger a restock and skip this sale
                    new_stock = inventory.restock(s_id, k_id)
                    logger.info(
                        "[RESTOCK] store=%s sku=%s stock→%d", s_id, k_id, new_stock
                    )
                    continue

                order = make_order_event(
                    s_id, k_id, sku.category, sku.unit_price, qty, is_anomaly
                )
                inv_evt = make_inventory_event(s_id, k_id, qty, on_hand, is_anomaly)

                producer.send(TOPIC_ORDERS, key=k_id, value=order.model_dump(mode="json"))
                producer.send(TOPIC_INVENTORY, key=k_id, value=inv_evt.model_dump(mode="json"))
                events_sent += 1

                if events_sent % 100 == 0:
                    logger.info("Events sent: %d", events_sent)

            # --- Sleep ---
            if mode == "realtime":
                gap = next_inter_arrival(now)
                time.sleep(gap)
            else:
                # fast: tiny sleep so real clock advances and event-time
                # timestamps span >65 s — required for 1-min windows to fire
                time.sleep(0.005)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="VeloShelf synthetic event generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["realtime", "fast"],
        default="realtime",
        help=(
            "realtime: sleep between events to match Poisson arrival rates (default). "
            "fast: no sleep — floods Kafka immediately, useful for testing."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(mode=args.mode)