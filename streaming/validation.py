"""Event validation for VeloShelf streaming pipeline.

Validates incoming order and inventory events against:
  - Required field presence (schema-level, handled by pydantic in the generator)
  - Business range rules (quantity > 0, price >= 0, on_hand >= 0)
  - Reference integrity (store_id and sku_id exist in seed dimensions)
  - Timestamp sanity (not more than 60s in the future)

This module is intentionally pure Python with no PyFlink imports so it can
be unit-tested without a Flink runtime. The Flink job (job.py) wraps these
functions in a MapFunction / ProcessFunction.

Returns a ValidationResult so callers decide whether to route to the
main stream or the dead-letter topic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    valid: bool
    event: dict[str, Any]
    reason: str = ""          # populated when valid=False


# ---------------------------------------------------------------------------
# Allowed reference sets (loaded once at job startup from seed CSVs)
# ---------------------------------------------------------------------------

class ReferenceData:
    """Holds the valid store_id and sku_id sets for reference checks."""

    def __init__(self, store_ids: set[str], sku_ids: set[str]) -> None:
        self.store_ids = store_ids
        self.sku_ids = sku_ids

    @classmethod
    def from_seed_loader(cls) -> ReferenceData:
        from generator.seed_loader import load_skus, load_stores
        return cls(
            store_ids={s.store_id for s in load_stores()},
            sku_ids={s.sku_id for s in load_skus()},
        )


# ---------------------------------------------------------------------------
# Validation rules
# ---------------------------------------------------------------------------

_MAX_FUTURE_DRIFT_S = 60.0   # events more than 60s in the future are rejected


def _parse_event_time(event: dict[str, Any]) -> datetime | None:
    raw = event.get("event_time")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def validate_order_event(
    event: dict[str, Any],
    ref: ReferenceData,
) -> ValidationResult:
    """Validate a raw order event dict.

    Rules:
      1. Required fields present.
      2. quantity > 0, unit_price >= 0.
      3. store_id and sku_id in reference sets.
      4. event_time parseable and not more than 60s in the future.
    """
    required = {"event_id", "event_time", "store_id", "sku_id",
                "category", "quantity", "unit_price", "order_id"}
    missing = required - event.keys()
    if missing:
        return ValidationResult(False, event, f"missing fields: {missing}")

    if event.get("quantity", 0) <= 0:
        return ValidationResult(False, event, f"quantity must be > 0, got {event['quantity']}")

    if event.get("unit_price", -1) < 0:
        return ValidationResult(False, event, f"unit_price must be >= 0, got {event['unit_price']}")

    if event["store_id"] not in ref.store_ids:
        return ValidationResult(False, event, f"unknown store_id: {event['store_id']}")

    if event["sku_id"] not in ref.sku_ids:
        return ValidationResult(False, event, f"unknown sku_id: {event['sku_id']}")

    event_time = _parse_event_time(event)
    if event_time is None:
        return ValidationResult(False, event, f"unparseable event_time: {event.get('event_time')}")

    now = datetime.now(tz=timezone.utc)
    if event_time > now + timedelta(seconds=_MAX_FUTURE_DRIFT_S):
        return ValidationResult(False, event, f"event_time too far in future: {event_time}")

    return ValidationResult(True, event)


def validate_inventory_event(
    event: dict[str, Any],
    ref: ReferenceData,
) -> ValidationResult:
    """Validate a raw inventory event dict.

    Rules:
      1. Required fields present.
      2. on_hand_after >= 0.
      3. movement_type in allowed values.
      4. store_id and sku_id in reference sets.
      5. event_time parseable and not more than 60s in the future.
    """
    required = {"event_id", "event_time", "store_id", "sku_id",
                "movement_type", "delta_units", "on_hand_after"}
    missing = required - event.keys()
    if missing:
        return ValidationResult(False, event, f"missing fields: {missing}")

    if event.get("on_hand_after", -1) < 0:
        return ValidationResult(
            False, event, f"on_hand_after must be >= 0, got {event['on_hand_after']}"
        )

    allowed_movements = {"sale", "restock", "adjustment"}
    if event.get("movement_type") not in allowed_movements:
        return ValidationResult(
            False, event, f"unknown movement_type: {event.get('movement_type')}"
        )

    if event["store_id"] not in ref.store_ids:
        return ValidationResult(False, event, f"unknown store_id: {event['store_id']}")

    if event["sku_id"] not in ref.sku_ids:
        return ValidationResult(False, event, f"unknown sku_id: {event['sku_id']}")

    event_time = _parse_event_time(event)
    if event_time is None:
        return ValidationResult(False, event, f"unparseable event_time: {event.get('event_time')}")

    now = datetime.now(tz=timezone.utc)
    if event_time > now + timedelta(seconds=_MAX_FUTURE_DRIFT_S):
        return ValidationResult(False, event, f"event_time too far in future: {event_time}")

    return ValidationResult(True, event)


def to_dead_letter(result: ValidationResult, topic_source: str) -> dict[str, Any]:
    """Wrap a failed event in a dead-letter envelope."""
    return {
        "source_topic": topic_source,
        "reason": result.reason,
        "failed_at": datetime.now(tz=timezone.utc).isoformat(),
        "original_event": json.dumps(result.event),
    }