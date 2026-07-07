"""Sink helpers for VeloShelf streaming pipeline.

PostgresSink  — upserts windowed feature rows; inserts alert rows.
KafkaAlertSink — publishes alert dicts to the appropriate Kafka alert topic.

Both classes are designed to be instantiated inside Flink's open() method
(once per task-manager slot) so connections are re-used across window outputs.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger("veloshelf.sinks")

TOPIC_STOCKOUT = os.getenv("TOPIC_STOCKOUT_ALERTS", "stockout-alerts")
TOPIC_SURGE    = os.getenv("TOPIC_SURGE_ALERTS",    "surge-alerts")


class PostgresSink:
    """Writes feature rows and alert rows to Postgres via psycopg2."""

    _UPSERT_FEATURES = """
        INSERT INTO windowed_features
            (store_id, sku_id, window_start, window_end,
             order_rate, depletion_vel, demand_momentum, on_hand_est, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (store_id, sku_id, window_start)
        DO UPDATE SET
            window_end      = EXCLUDED.window_end,
            order_rate      = EXCLUDED.order_rate,
            depletion_vel   = EXCLUDED.depletion_vel,
            demand_momentum = EXCLUDED.demand_momentum,
            on_hand_est     = EXCLUDED.on_hand_est,
            updated_at      = EXCLUDED.updated_at;
    """

    _INSERT_ALERT = """
        INSERT INTO alerts
            (alert_id, alert_type, store_id, sku_id,
             triggered_at, metric_value, threshold, resolved)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (alert_id) DO NOTHING;
    """

    def __init__(self, dsn: str) -> None:
        import psycopg2
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True

    def upsert_features(self, row: dict[str, Any]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(self._UPSERT_FEATURES, (
                row["store_id"], row["sku_id"],
                row["window_start"], row["window_end"],
                row["order_rate"], row["depletion_vel"],
                row["demand_momentum"], row["on_hand_est"],
                row["updated_at"],
            ))

    def insert_alert(self, alert: dict[str, Any]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(self._INSERT_ALERT, (
                alert["alert_id"], alert["alert_type"],
                alert["store_id"], alert["sku_id"],
                alert["triggered_at"], alert["metric_value"],
                alert["threshold"], alert["resolved"],
            ))

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()


class KafkaAlertSink:
    """Publishes alert dicts to the Kafka stockout-alerts / surge-alerts topics."""

    def __init__(self, bootstrap_servers: str) -> None:
        from kafka import KafkaProducer
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, alert: dict[str, Any]) -> None:
        topic = (
            TOPIC_STOCKOUT
            if alert.get("alert_type") == "stockout_risk"
            else TOPIC_SURGE
        )
        self._producer.send(topic, alert)

    def flush(self) -> None:
        self._producer.flush()
