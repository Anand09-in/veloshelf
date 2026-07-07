"""Export windowed_features from Postgres to Parquet for ML training.

Usage:
    python -m ml.export_features          # writes to data/features/features.parquet
    FEATURES_PATH=s3://... python -m ml.export_features

Environment variables:
    POSTGRES_DSN   (default: postgresql://veloshelf:veloshelf@localhost:5432/veloshelf)
    FEATURES_PATH  (default: data/features)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
logger = logging.getLogger("veloshelf.export_features")

POSTGRES_DSN  = os.getenv("POSTGRES_DSN",  "postgresql://veloshelf:veloshelf@localhost:5432/veloshelf")
FEATURES_PATH = Path(os.getenv("FEATURES_PATH", "data/features"))


def export() -> None:
    logger.info("Connecting to Postgres...")
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        logger.info("Reading windowed_features...")
        cur.execute("SELECT * FROM windowed_features ORDER BY updated_at")
        rows = cur.fetchall()
        cols = [desc.name for desc in cur.description]

    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        logger.error("windowed_features is empty — run the generator + Flink job first.")
        raise SystemExit(1)

    FEATURES_PATH.mkdir(parents=True, exist_ok=True)
    out = FEATURES_PATH / "features.parquet"
    df.to_parquet(out, index=False)
    logger.info("Exported %d rows → %s", len(df), out)


if __name__ == "__main__":
    export()
