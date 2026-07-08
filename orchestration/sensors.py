"""Dagster sensors for VeloShelf (Phase 5).

DriftRetrainSensor:
  - Runs every 5 minutes (minimum_interval_seconds=300).
  - Reads the latest drift_report asset materialization metadata.
  - Calls retrain_trigger.should_retrain() to decide.
  - If triggered: fires detector_retrain_job and/or forecaster_retrain_job.
  - Records the trigger timestamp (cooldown) so it doesn't fire again immediately.

The sensor does NOT run the drift job itself — that's the drift_check_schedule
(every 2h). The sensor only reacts to the output of the most recent drift run.

This design separates concerns cleanly:
  drift_check_schedule → produces drift_report asset
  DriftRetrainSensor   → reacts to drift_report → triggers retrain jobs
"""

from __future__ import annotations

import json
import logging

from dagster import (
    AssetKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from observability.retrain_trigger import record_trigger, should_retrain
from orchestration.schedules import detector_job, forecaster_job

logger = logging.getLogger(__name__)

# How often the sensor polls (seconds). 5 minutes is reasonable —
# drift jobs run every 2h so polling faster than that wastes cycles.
_POLL_INTERVAL_S = 300


@sensor(
    jobs=[detector_job, forecaster_job],
    minimum_interval_seconds=_POLL_INTERVAL_S,
    description=(
        "Watches drift_report asset materializations. "
        "Triggers detector and/or forecaster retraining when PSI exceeds threshold."
    ),
)
def drift_retrain_sensor(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    """Sensor: drift_report → retrain if PSI > threshold and not in cooldown."""

    # Read cursor — tracks the last drift_report run_id we processed
    last_processed_run_id = context.cursor or ""

    # Fetch the latest drift_report asset materialization from Dagster's event log
    try:
        instance = context.instance

        # Get the latest materialization event for the drift_report asset
        _drift_key = AssetKey("drift_report")
        materializations = instance.get_latest_materialization_events(
            asset_keys=[_drift_key]
        )

        drift_mat = materializations.get(_drift_key)

        if drift_mat is None:
            return SkipReason("No drift_report materialization found yet. "
                              "Run the drift_check_schedule first.")

        # Check if we've already processed this materialization
        run_id = drift_mat.run_id
        if run_id == last_processed_run_id:
            return SkipReason(f"Already processed drift_report run {run_id}. "
                              "Waiting for next drift run.")

        # Extract drift summary from materialization metadata
        metadata = drift_mat.asset_materialization.metadata
        drift_summary_raw = metadata.get("drift_summary")

        if drift_summary_raw is None:
            # Fallback: re-run the drift job inline to get the summary
            context.log.info("No drift_summary in metadata — running drift_job inline.")
            from observability.drift_job import run_drift_job
            drift_summary = run_drift_job()
        else:
            drift_summary = json.loads(str(drift_summary_raw))

    except Exception as e:
        context.log.warning("Could not read drift_report materialization: %s", e)
        # Fallback: run drift job inline
        try:
            from observability.drift_job import run_drift_job
            drift_summary = run_drift_job()
            run_id = "inline"
        except Exception as e2:
            return SkipReason(f"Drift job failed: {e2}")

    # Decision
    decision = should_retrain(drift_summary)
    context.log.info("Trigger decision: %s | reason: %s", decision.should_trigger, decision.reason)

    if not decision.should_trigger:
        context.set_cursor(run_id)
        return SkipReason(decision.reason)

    # Build run requests
    run_requests = []
    tags = {
        "triggered_by":     "drift_retrain_sensor",
        "drifted_features": ",".join(decision.drifted_features),
        "max_psi":          f"{decision.max_psi:.4f}",
    }

    if decision.trigger_detector:
        run_requests.append(RunRequest(
            run_key=f"detector_retrain_{run_id}",
            job_name="detector_retrain_job",
            tags=tags,
        ))
        record_trigger("detector_retrain")
        context.log.info("Queuing detector_retrain_job.")

    if decision.trigger_forecaster:
        run_requests.append(RunRequest(
            run_key=f"forecaster_retrain_{run_id}",
            job_name="forecaster_retrain_job",
            tags=tags,
        ))
        record_trigger("forecaster_retrain")
        context.log.info("Queuing forecaster_retrain_job.")

    context.set_cursor(run_id)
    return SensorResult(run_requests=run_requests)