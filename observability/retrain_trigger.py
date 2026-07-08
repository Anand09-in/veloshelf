"""VeloShelf retrain trigger logic (Phase 5).

Pure Python — no Dagster imports — so it can be unit-tested without
a Dagster runtime and reused by other trigger mechanisms.

Decision logic:
  1. Read the latest drift_report result (dict returned by drift_job.run_drift_job).
  2. Check if any feature PSI exceeds PSI_THRESHOLD.
  3. Check if the cooldown period has passed since the last triggered retrain.
  4. If both conditions met → return TriggerDecision(should_trigger=True).

Cooldown is enforced via a local sentinel file (same mechanism as promote.py).
This prevents a persistent drift signal from triggering infinite retrains.

Separate cooldowns for detector and forecaster:
  - Detector:   DETECTOR_COOLDOWN_MINUTES  (default 60)
  - Forecaster: FORECASTER_COOLDOWN_MINUTES (default 120)
  Both can be overridden via env vars.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

PSI_THRESHOLD             = float(os.getenv("PSI_DRIFT_THRESHOLD",        "0.25"))
DETECTOR_COOLDOWN_MINUTES  = int(os.getenv("DETECTOR_COOLDOWN_MINUTES",   "60"))
FORECASTER_COOLDOWN_MINUTES = int(os.getenv("FORECASTER_COOLDOWN_MINUTES", "120"))

_COOLDOWN_DIR = Path(".retrain_cooldowns")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class TriggerDecision:
    should_trigger: bool
    reason: str
    drifted_features: list[str]
    max_psi: float
    trigger_detector: bool = False
    trigger_forecaster: bool = False


# ---------------------------------------------------------------------------
# Cooldown helpers
# ---------------------------------------------------------------------------

def _cooldown_path(job_name: str) -> Path:
    _COOLDOWN_DIR.mkdir(exist_ok=True)
    return _COOLDOWN_DIR / f"{job_name}.last_triggered"


def _in_cooldown(job_name: str, cooldown_minutes: int) -> bool:
    path = _cooldown_path(job_name)
    if not path.exists():
        return False
    last = float(path.read_text().strip())
    elapsed_min = (time.time() - last) / 60.0
    if elapsed_min < cooldown_minutes:
        logger.info(
            "Cooldown active for %s: %.1f / %d min elapsed.",
            job_name, elapsed_min, cooldown_minutes,
        )
        return True
    return False


def record_trigger(job_name: str) -> None:
    """Record that a retrain was triggered now (resets the cooldown clock)."""
    _cooldown_path(job_name).write_text(str(time.time()))


def clear_cooldown(job_name: str) -> None:
    """Remove cooldown sentinel — useful for testing or manual override."""
    path = _cooldown_path(job_name)
    if path.exists():
        path.unlink()
    logger.info("Cooldown cleared for %s.", job_name)


# ---------------------------------------------------------------------------
# Core decision function
# ---------------------------------------------------------------------------

def should_retrain(drift_summary: dict) -> TriggerDecision:
    """Evaluate a drift_job summary dict and decide whether to trigger retraining.

    Args:
        drift_summary: dict returned by observability.drift_job.run_drift_job().
                       Key fields: skipped, any_drift, drift_results.

    Returns:
        TriggerDecision with full reasoning.
    """
    # Skip check
    if drift_summary.get("skipped"):
        return TriggerDecision(
            should_trigger=False,
            reason=f"drift_job skipped: {drift_summary.get('reason', 'unknown')}",
            drifted_features=[],
            max_psi=0.0,
        )

    if not drift_summary.get("any_drift", False):
        return TriggerDecision(
            should_trigger=False,
            reason="No drift detected (all PSI below threshold).",
            drifted_features=[],
            max_psi=max(
                (m.get("psi", 0.0) for m in drift_summary.get("drift_results", {}).values()),
                default=0.0,
            ),
        )

    # Identify drifted features and max PSI
    drift_results = drift_summary.get("drift_results", {})
    drifted = [f for f, m in drift_results.items() if m.get("drifted")]
    max_psi = max((m.get("psi", 0.0) for m in drift_results.values()), default=0.0)

    # Cooldown checks
    detector_blocked   = _in_cooldown("detector_retrain",   DETECTOR_COOLDOWN_MINUTES)
    forecaster_blocked = _in_cooldown("forecaster_retrain", FORECASTER_COOLDOWN_MINUTES)

    trigger_detector   = not detector_blocked
    trigger_forecaster = not forecaster_blocked

    if not trigger_detector and not trigger_forecaster:
        return TriggerDecision(
            should_trigger=False,
            reason=(
                f"Drift detected on {drifted} (max PSI={max_psi:.4f}) "
                "but both jobs are in cooldown."
            ),
            drifted_features=drifted,
            max_psi=max_psi,
        )

    # At least one job should trigger
    triggered_jobs = []
    if trigger_detector:
        triggered_jobs.append("detector")
    if trigger_forecaster:
        triggered_jobs.append("forecaster")

    return TriggerDecision(
        should_trigger=True,
        reason=(
            f"Drift detected on features {drifted} "
            f"(max PSI={max_psi:.4f} > threshold={PSI_THRESHOLD}). "
            f"Triggering: {triggered_jobs}."
        ),
        drifted_features=drifted,
        max_psi=max_psi,
        trigger_detector=trigger_detector,
        trigger_forecaster=trigger_forecaster,
    )