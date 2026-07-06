"""Event and dimension schemas for VeloShelf.

These are the data contracts shared across the pipeline. The generator (Phase 1)
produces OrderEvent / InventoryEvent; the Flink job (Phase 2) validates against
these shapes and quarantines anything that fails.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class MovementType(str, Enum):
    SALE = "sale"
    RESTOCK = "restock"
    ADJUSTMENT = "adjustment"


class OrderEvent(BaseModel):
    """A single customer order line for one SKU at one dark store."""

    event_id: str
    event_time: datetime
    store_id: str
    sku_id: str
    category: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    order_id: str
    is_injected_anomaly: bool = False


class InventoryEvent(BaseModel):
    """A stock movement for one SKU at one dark store."""

    event_id: str
    event_time: datetime
    store_id: str
    sku_id: str
    movement_type: MovementType
    delta_units: int
    on_hand_after: int = Field(ge=0)
    is_injected_anomaly: bool = False


class DimSku(BaseModel):
    sku_id: str
    name: str
    category: str
    unit_price: float = Field(ge=0)
    reorder_point: int = Field(ge=0)


class DimStore(BaseModel):
    store_id: str
    region: str
    capacity: int = Field(ge=0)