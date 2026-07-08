"""Dagster schedules for VeloShelf ML pipeline (Phase 3).

Schedule design:
  - Detector retraining:  every 6 hours (enough data accumulates per run)
  - Forecaster retraining: every 6 hours (same cadence, offset by 30 min)

In Phase 5, drift-triggered retraining supplements these schedules —
the drift sensor fires a run outside the schedule when PSI > threshold.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)

from orchestration.assets import (
    detector_promotion,
    detector_training_run,
    drift_report,
    forecaster_promotion,
    forecaster_training_run,
    windowed_features_parquet,
)

# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

detector_job = define_asset_job(
    name="detector_retrain_job",
    selection=AssetSelection.assets(
        windowed_features_parquet,
        detector_training_run,
        detector_promotion,
    ),
    description="Retrain + promote the Isolation Forest anomaly detector.",
)

forecaster_job = define_asset_job(
    name="forecaster_retrain_job",
    selection=AssetSelection.assets(
        windowed_features_parquet,
        forecaster_training_run,
        forecaster_promotion,
    ),
    description="Retrain + promote the XGBoost demand forecaster.",
)

 
drift_job = define_asset_job(
    name="drift_check_job",
    selection=AssetSelection.assets(
        windowed_features_parquet,
        drift_report,
    ),
    description="Run Evidently drift detection on rolling feature windows.",
)

# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

detector_schedule = ScheduleDefinition(
    job=detector_job,
    cron_schedule="0 */6 * * *",    # every 6 hours, on the hour
    name="detector_retrain_schedule",
)

forecaster_schedule = ScheduleDefinition(
    job=forecaster_job,
    cron_schedule="30 */6 * * *",   # every 6 hours, offset by 30 min
    name="forecaster_retrain_schedule",
)


 
drift_schedule = ScheduleDefinition(
    job=drift_job,
    cron_schedule="0 */2 * * *",   # every 2 hours
    name="drift_check_schedule",
)