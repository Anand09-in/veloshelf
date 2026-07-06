"""Load and validate the seed dimension CSVs.

Run via `make seed`. Proves seed data parses cleanly against the schemas
before Phase 1 starts producing events.
"""

from __future__ import annotations

import csv
from pathlib import Path

from generator.schemas import DimSku, DimStore

SEED_DIR = Path(__file__).resolve().parent.parent / "data" / "seeds"


def load_skus() -> list[DimSku]:
    with (SEED_DIR / "dim_sku.csv").open() as f:
        return [DimSku(**row) for row in csv.DictReader(f)]


def load_stores() -> list[DimStore]:
    with (SEED_DIR / "dim_store.csv").open() as f:
        return [DimStore(**row) for row in csv.DictReader(f)]


def main() -> None:
    skus = load_skus()
    stores = load_stores()
    print(f"Loaded {len(skus)} SKUs across "
          f"{len({s.category for s in skus})} categories.")
    print(f"Loaded {len(stores)} dark stores.")
    print("Seed validation OK.")


if __name__ == "__main__":
    main()