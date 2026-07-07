"""Hot-swap model loader for VeloShelf online scoring.

Polls the MLflow model registry at a configurable interval and reloads
the Production model when a new version is detected. Used by the Flink
FeatureSinkFn to score each feature row with the current best model.

Design:
  - Thread-safe: model pointer replaced atomically via a lock.
  - Fallback: if no Production model exists or registry is unreachable,
    falls back to the rule-based scorer from Phase 2.
  - Poll interval: MODEL_POLL_INTERVAL_S (default 300s / 5 min).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import mlflow

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_POLL_INTERVAL_S = int(os.getenv("MODEL_POLL_INTERVAL_S", "300"))

# MLflow registered model names
DETECTOR_MODEL_NAME  = os.getenv("DETECTOR_MODEL_NAME",  "veloshelf-anomaly-detector")
FORECASTER_MODEL_NAME = os.getenv("FORECASTER_MODEL_NAME", "veloshelf-demand-forecaster")


class HotSwapModelLoader:
    """Loads and periodically refreshes a model from the MLflow registry.

    Usage:
        loader = HotSwapModelLoader(DETECTOR_MODEL_NAME)
        loader.start_polling()
        model = loader.get_model()   # always returns current production model
    """

    def __init__(self, model_name: str, poll_interval_s: int = MODEL_POLL_INTERVAL_S) -> None:
        self._model_name = model_name
        self._poll_interval = poll_interval_s
        self._model: Any = None
        self._version: str | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        self._try_load()    # eager load on construction

    def _try_load(self) -> None:
        """Attempt to load the current Production model from the registry."""
        try:
            from mlflow.tracking import MlflowClient
            client = MlflowClient()
            versions = client.get_latest_versions(self._model_name, stages=["Production"])
            if not versions:
                logger.warning("No Production version found for %s.", self._model_name)
                return
            latest_version = versions[0].version
            if latest_version == self._version:
                return   # no change

            model_uri = f"models:/{self._model_name}/Production"
            new_model = mlflow.sklearn.load_model(model_uri)

            with self._lock:
                self._model = new_model
                self._version = latest_version

            logger.info(
                "Hot-swapped %s → version %s", self._model_name, latest_version
            )
        except Exception as e:
            logger.warning("Could not load model %s: %s", self._model_name, e)

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self._poll_interval)
            self._try_load()

    def start_polling(self) -> None:
        """Start background polling thread. Call once at job startup."""
        t = threading.Thread(target=self._poll_loop, daemon=True)
        t.start()
        logger.info(
            "Started polling for %s every %ds", self._model_name, self._poll_interval
        )

    def get_model(self) -> Any | None:
        """Return the current production model, or None if unavailable."""
        with self._lock:
            return self._model

    def is_ready(self) -> bool:
        """Return True if a model is loaded and ready for inference."""
        with self._lock:
            return self._model is not None

    def stop(self) -> None:
        self._stop.set()