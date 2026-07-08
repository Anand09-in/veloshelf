"""Phase 5 unit tests — retrain trigger logic.

Tests cover the pure Python trigger decision logic in retrain_trigger.py.
No Dagster runtime required.
"""

from __future__ import annotations

import time

import pytest

from observability.retrain_trigger import (
    _in_cooldown,
    clear_cooldown,
    record_trigger,
    should_retrain,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_cooldowns(tmp_path, monkeypatch):
    """Redirect cooldown files to tmp_path so tests don't interfere."""
    import observability.retrain_trigger as rt
    monkeypatch.setattr(rt, "_COOLDOWN_DIR", tmp_path / "cooldowns")
    yield


def _drift_summary(
    any_drift: bool = True,
    features: list[str] | None = None,
    psi: float = 0.30,
) -> dict:
    """Build a minimal drift_summary dict."""
    features = features or ["order_rate"]
    drift_results = {
        f: {
            "psi": psi,
            "ks_stat": 0.4,
            "ks_pval": 0.01,
            "js_div": 0.15,
            "drifted": float(psi > 0.25),
        }
        for f in features
    }
    return {
        "skipped": False,
        "any_drift": any_drift,
        "drift_results": drift_results,
        "n_current": 60,
        "n_reference": 300,
    }


# ---------------------------------------------------------------------------
# should_retrain — basic cases
# ---------------------------------------------------------------------------

class TestShouldRetrain:
    def test_no_drift_does_not_trigger(self):
        summary = _drift_summary(any_drift=False, psi=0.05)
        decision = should_retrain(summary)
        assert not decision.should_trigger
        assert "No drift" in decision.reason

    def test_drift_triggers_both_jobs(self):
        decision = should_retrain(_drift_summary(any_drift=True, psi=0.35))
        assert decision.should_trigger
        assert decision.trigger_detector
        assert decision.trigger_forecaster

    def test_skipped_job_does_not_trigger(self):
        summary = {"skipped": True, "reason": "insufficient_current_data"}
        decision = should_retrain(summary)
        assert not decision.should_trigger
        assert "skipped" in decision.reason

    def test_drifted_features_listed(self):
        summary = _drift_summary(any_drift=True, features=["order_rate", "depletion_vel"])
        decision = should_retrain(summary)
        assert "order_rate" in decision.drifted_features
        assert "depletion_vel" in decision.drifted_features

    def test_max_psi_correct(self):
        summary = _drift_summary(any_drift=True, psi=0.42)
        decision = should_retrain(summary)
        assert abs(decision.max_psi - 0.42) < 0.01

    def test_trigger_decision_has_reason(self):
        decision = should_retrain(_drift_summary(any_drift=True))
        assert len(decision.reason) > 0


# ---------------------------------------------------------------------------
# Cooldown logic
# ---------------------------------------------------------------------------

class TestCooldown:
    def test_no_cooldown_initially(self):
        assert not _in_cooldown("test_job", cooldown_minutes=60)

    def test_cooldown_active_after_record(self):
        record_trigger("test_job")
        assert _in_cooldown("test_job", cooldown_minutes=60)

    def test_cooldown_not_active_after_clear(self):
        record_trigger("test_job")
        clear_cooldown("test_job")
        assert not _in_cooldown("test_job", cooldown_minutes=60)

    def test_cooldown_not_active_with_zero_minutes(self):
        record_trigger("test_job")
        # zero cooldown = always expired
        assert not _in_cooldown("test_job", cooldown_minutes=0)

    def test_drift_blocked_by_cooldown(self):
        """When both jobs are in cooldown, should_retrain returns False."""
        record_trigger("detector_retrain")
        record_trigger("forecaster_retrain")
        decision = should_retrain(_drift_summary(any_drift=True))
        assert not decision.should_trigger
        assert "cooldown" in decision.reason

    def test_only_detector_blocked(self, monkeypatch):
        """When only detector is in cooldown, forecaster should still trigger."""
        import observability.retrain_trigger as rt
        monkeypatch.setattr(rt, "DETECTOR_COOLDOWN_MINUTES", 60)
        monkeypatch.setattr(rt, "FORECASTER_COOLDOWN_MINUTES", 60)

        record_trigger("detector_retrain")
        # forecaster NOT triggered yet

        decision = should_retrain(_drift_summary(any_drift=True))
        assert decision.should_trigger
        assert not decision.trigger_detector    # in cooldown
        assert decision.trigger_forecaster      # not in cooldown

    def test_record_trigger_creates_file(self, tmp_path, monkeypatch):
        import observability.retrain_trigger as rt
        cooldown_dir = tmp_path / "cooldowns"
        monkeypatch.setattr(rt, "_COOLDOWN_DIR", cooldown_dir)
        record_trigger("my_job")
        sentinel = cooldown_dir / "my_job.last_triggered"
        assert sentinel.exists()
        ts = float(sentinel.read_text().strip())
        assert abs(ts - time.time()) < 5.0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_drift_results(self):
        summary = {
            "skipped": False,
            "any_drift": True,
            "drift_results": {},
        }
        decision = should_retrain(summary)
        # any_drift=True but no features listed — max_psi is 0
        assert decision.max_psi == 0.0

    def test_psi_exactly_at_threshold_does_not_drift(self):
        """PSI must EXCEED threshold, not equal it."""
        import observability.retrain_trigger as rt
        threshold = rt.PSI_THRESHOLD
        summary = _drift_summary(any_drift=False, psi=threshold)
        summary["drift_results"]["order_rate"]["drifted"] = 0.0
        summary["any_drift"] = False
        decision = should_retrain(summary)
        assert not decision.should_trigger

    def test_multiple_features_some_drifted(self):
        summary = {
            "skipped": False,
            "any_drift": True,
            "drift_results": {
                "order_rate":     {"psi": 0.35, "ks_stat": 0.4, "ks_pval": 0.01,
                                   "js_div": 0.2, "drifted": 1.0},
                "on_hand_est":    {"psi": 0.05, "ks_stat": 0.1, "ks_pval": 0.3,
                                   "js_div": 0.02, "drifted": 0.0},
                "depletion_vel":  {"psi": 0.28, "ks_stat": 0.35, "ks_pval": 0.02,
                                   "js_div": 0.18, "drifted": 1.0},
            },
        }
        decision = should_retrain(summary)
        assert decision.should_trigger
        assert "order_rate" in decision.drifted_features
        assert "depletion_vel" in decision.drifted_features
        assert "on_hand_est" not in decision.drifted_features
        assert abs(decision.max_psi - 0.35) < 0.01