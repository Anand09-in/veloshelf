"""Dagster entrypoint for VeloShelf (Phase 3 — real ML assets).

Wires together all assets and schedules so `dagster dev` shows the full
ML pipeline graph in the UI at http://localhost:3000.
"""

from __future__ import annotations

from dagster import Definitions

from orchestration.assets import (
    detector_promotion,
    detector_training_run,
    drift_report,
    forecaster_promotion,
    forecaster_training_run,
    windowed_features_parquet,
)
from orchestration.schedules import detector_schedule, drift_schedule, forecaster_schedule

defs = Definitions(
    assets=[
        windowed_features_parquet,
        detector_training_run,
        detector_promotion,
        forecaster_training_run,
        forecaster_promotion,
        drift_report,
    ],
    schedules=[
        detector_schedule,
        forecaster_schedule,
        drift_schedule,
    ],
)