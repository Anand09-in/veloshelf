from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")


def _delivery_report(err: Any, msg: Any) -> None:
    if err:
        logger.error("Delivery failed | topic=%s err=%s", msg.topic(), err)
    else:
        logger.debug("Delivered | topic=%s partition=%d offset=%d",
                     msg.topic(), msg.partition(), msg.offset())


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "acks": "all", "retries": 3}
        )

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=json.dumps(value, default=_json_default).encode(),
            callback=_delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def __enter__(self) -> KafkaProducerClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.flush()


@contextmanager
def kafka_producer(bootstrap_servers: str):
    client = KafkaProducerClient(bootstrap_servers)
    try:
        yield client
    finally:
        client.flush()