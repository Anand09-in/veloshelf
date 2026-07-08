"""Dagster entrypoint for VeloShelf (Phase 5 — drift sensor added).
 
Assets:
  windowed_features_parquet
    ├── detector_training_run  → detector_promotion
    ├── forecaster_training_run → forecaster_promotion
    └── drift_report
 
Schedules:
  detector_retrain   — every 6h
  forecaster_retrain — every 6h (+30min offset)
  drift_check        — every 2h
 
Sensors:
  drift_retrain_sensor — polls drift_report every 5min,
                         triggers retraining when PSI > threshold
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
from orchestration.sensors import drift_retrain_sensor

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
    sensors=[
        drift_retrain_sensor,
    ],
)