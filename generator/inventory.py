from __future__ import annotations

import random
from dataclasses import dataclass, field

from generator.schemas import DimSku, DimStore

_INITIAL_STOCK_MIN = 80
_INITIAL_STOCK_MAX = 150
_RESTOCK_MIN = 60
_RESTOCK_MAX = 120


@dataclass
class InventoryState:
    stock: dict[tuple[str, str], int] = field(default_factory=dict)

    @classmethod
    def initialise(
        cls, stores: list[DimStore], skus: list[DimSku], seed: int = 0
    ) -> InventoryState:
        rng = random.Random(seed)
        state = cls()
        for store in stores:
            for sku in skus:
                state.stock[(store.store_id, sku.sku_id)] = rng.randint(
                    _INITIAL_STOCK_MIN, _INITIAL_STOCK_MAX
                )
        return state

    def on_hand(self, store_id: str, sku_id: str) -> int:
        return self.stock.get((store_id, sku_id), 0)

    def sell(self, store_id: str, sku_id: str, quantity: int) -> int | None:
        key = (store_id, sku_id)
        current = self.stock.get(key, 0)
        if current < quantity:
            return None
        self.stock[key] = current - quantity
        return self.stock[key]

    def restock(self, store_id: str, sku_id: str) -> int:
        key = (store_id, sku_id)
        self.stock[key] = random.randint(_RESTOCK_MIN, _RESTOCK_MAX)
        return self.stock[key]

    def force_deplete(self, store_id: str, sku_id: str, target: int = 5) -> int:
        key = (store_id, sku_id)
        current = self.stock.get(key, 0)
        self.stock[key] = min(current, max(0, target))
        return self.stock[key]