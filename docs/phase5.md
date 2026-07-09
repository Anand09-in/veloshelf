# Phase 5 — Closed-Loop Retraining

A Dagster sensor that watches `drift_report` materializations and automatically queues retraining jobs when PSI exceeds threshold. Cooldown sentinel files prevent retrain storms. The Phase 3 validation gate ensures only better models get promoted.

---

## Files

| File | Role |
|---|---|
| `observability/retrain_trigger.py` | Pure-Python trigger decision + cooldown read/write |
| `orchestration/sensors.py` | Dagster `drift_retrain_sensor` — watches drift_report, requests retrain jobs |
| `orchestration/definitions.py` | Registers the sensor alongside assets and schedules |

---

## How the loop runs

```
drift_check_schedule (every 2h)
    └── drift_report asset materialises
            └── stores {psi, ks, js, any_drift} as Dagster asset metadata

drift_retrain_sensor (evaluates every 5 min)
    └── reads latest drift_report metadata
            ├── any_drift=False → SkipReason("no drift")
            ├── cooldown active  → SkipReason("cooldown until <timestamp>")
            └── any_drift=True + no cooldown
                    ├── RunRequest(job=detector_retrain_job)
                    ├── RunRequest(job=forecaster_retrain_job)
                    └── writes cooldown sentinel files

detector_retrain_job / forecaster_retrain_job
    └── export_features → train → validate → promote (if better)

HotSwapModelLoader in Flink (polls every 5 min)
    └── detects new Production version → swaps in-memory model
```

Total lag from drift detection to new model live: **10–20 minutes** (drift job ~30s, sensor tick up to 5 min, training ~2–5 min, Flink hot-swap up to 5 min).

---

## Trigger logic — `observability/retrain_trigger.py`

`should_retrain(model_name, drift_summary, cooldown_hours=4)`:

1. Read `drift_summary["any_drift"]` — if False, return `(False, "no drift")`
2. Check `.retrain_cooldowns/{model_name}.last_triggered` — if the file exists and was written within `cooldown_hours`, return `(False, "cooldown active until {timestamp}")`
3. Otherwise return `(True, "drift detected, PSI={max_psi}")`

`record_trigger(model_name)`:
- Writes the current UTC timestamp to `.retrain_cooldowns/{model_name}.last_triggered`
- Creates the `.retrain_cooldowns/` directory if it doesn't exist

The sentinel files are plain text (ISO-8601 timestamp). They survive Dagster restarts and container restarts because `.retrain_cooldowns/` is on the mounted volume (`.:/app` in docker-compose).

---

## Dagster sensor — `orchestration/sensors.py`

```python
@sensor(job=[detector_retrain_job, forecaster_retrain_job], minimum_interval_seconds=300)
def drift_retrain_sensor(context):
    ...
```

On each evaluation (every 5 min):

1. Load the most recent `drift_report` asset materialisation record from Dagster's event log
2. Extract the drift summary from the materialisation metadata
3. If no recent materialisation (older than 3h), yield `SkipReason`
4. For each model (`detector`, `forecaster`):
   - Call `should_retrain(model_name, drift_summary)`
   - If True: `record_trigger(model_name)` + `yield RunRequest(run_key=..., job_name=...)`
5. If neither retrained: yield `SkipReason` with reason

`run_key` is set to `f"{model_name}_{drift_report_run_id}"` so the same drift report never triggers duplicate retrain runs even if the sensor evaluates multiple times before training completes.

**Fallback**: if `drift_report` metadata is absent (e.g., the drift job ran outside Dagster via `python -m observability.drift_job`), the sensor calls `drift_job.run()` inline to get fresh data. This makes the sensor robust to out-of-band runs.

---

## Cooldown design

Two guardrails operate independently:

**Cooldown (Phase 5)** — prevents retrain storms. A noisy or persistent drift signal could trigger retraining on every sensor tick. 4-hour cooldown means at most 6 retrains per day per model, regardless of how frequently drift is detected.

**Validation gate (Phase 3)** — prevents silent degradation. Even if retraining fires, `ml/promote.py` will not promote the new model unless it beats the current Production incumbent. Drift can cause retraining to produce a *worse* model on the new distribution; the gate catches this.

The two guardrails cover different failure modes and are not redundant.

---

## Sentinel files

```
.retrain_cooldowns/
├── detector_retrain.last_triggered     # "2024-01-15T14:32:11Z"
└── forecaster_retrain.last_triggered   # "2024-01-15T14:35:04Z"
```

To force a retrain immediately (bypass cooldown for testing):
```bash
rm .retrain_cooldowns/*.last_triggered
```

Then manually evaluate the sensor in Dagster UI → Automation → Sensors → `drift_retrain_sensor` → Evaluate.

---

## End-to-end verification

```bash
# 1. Start generator to produce features
python -m generator.producer --mode fast   # run ~3 min

# 2. Run drift job directly (or wait for Dagster schedule)
python -m observability.drift_job

# 3. Check Dagster sensor in UI
# http://localhost:3000 → Automation → Sensors → drift_retrain_sensor
# Click "Evaluate" if any_drift=True and no cooldown active

# 4. Watch retrain jobs in Run History
# http://localhost:3000 → Runs

# 5. Check MLflow for new model version
# http://localhost:5000 → Models → veloshelf-anomaly-detector

# 6. Confirm Flink hot-swap (within 5 min of promotion)
docker compose logs flink-taskmanager | grep "Hot-swapped"
```

---

## Interview talking points

**"Why sensor-triggered (not always-on retrain)?"**
Retrain only when data distribution has actually shifted — not on a fixed clock. This is more cost-efficient (training jobs are the most expensive step), avoids unnecessary model churn, and makes the retrain signal meaningful. The drift measurement is the justification for the retrain, not just a coincidence.

**"How do you prevent a cascade of retrains?"**
Two mechanisms: the 4-hour cooldown prevents the same drift signal from triggering back-to-back runs; and the `run_key` on each `RunRequest` prevents Dagster from launching duplicate runs for the same drift event if the sensor evaluates multiple times before the first training completes.

**"What if training makes the model worse?"**
The validation gate in `ml/promote.py` handles this — the new model must beat the incumbent on holdout metrics before the `Production` alias is updated. The Flink scorer keeps using the old model until a better one is promoted. A retrain that doesn't improve quality is logged in MLflow as a rejected run.
