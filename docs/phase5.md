# Phase 5 — Closed-Loop Retraining (Drift-Triggered)

> Goal: a Dagster sensor that watches drift_report materializations and
> automatically triggers retraining when PSI exceeds threshold,
> with a cooldown guard to prevent retrain storms.

**Definition of done:**
- `make test` passes all Phase 5 tests.
- Dagster UI shows `drift_retrain_sensor` under Automation → Sensors.
- Sensor can be turned on and evaluated manually in the UI.
- When drift is detected, the sensor queues `detector_retrain_job`
  and/or `forecaster_retrain_job` runs visible in Dagster Run History.
- Cooldown prevents re-triggering within the configured window.

---

## Task list

### New files
- [x] `observability/retrain_trigger.py` — pure Python trigger decision + cooldown logic
- [x] `orchestration/sensors.py`         — Dagster sensor watching drift_report
- [x] `tests/test_phase5.py`             — unit tests for trigger logic + cooldowns

### Updated files
- [x] `orchestration/definitions.py`     — registers drift_retrain_sensor

### Verification steps
- [ ] `make test`  — all tests pass (88 + new phase5 tests)
- [ ] `make lint`  — ruff clean
- [ ] Dagster UI → Automation → Sensors shows `drift_retrain_sensor`
- [ ] Turn sensor ON in Dagster UI
- [ ] Manually trigger drift_check_job to produce a fresh drift_report
- [ ] If drift detected → sensor fires → retrain runs appear in Run History
- [ ] Verify cooldown: sensor does not fire again immediately after triggering
- [ ] Check `.retrain_cooldowns/` directory for sentinel files

---

## Step-by-step verification

### 1 — Tests and lint
```bash
make test
make lint
```

### 2 — Restart Dagster to pick up sensor
```bash
docker-compose restart dagster
# or if running locally:
# restart: dagster dev -f orchestration/definitions.py
```

### 3 — Enable the sensor in Dagster UI
Open http://localhost:3000 → Automation → Sensors

You should see `drift_retrain_sensor`. Click it → **Start sensor**.

### 4 — Manually trigger a drift run to test the loop

Option A — via Dagster UI:
- Jobs → `drift_check_job` → Launch Run

Option B — via CLI:
```bash
python -m observability.drift_job
```

### 5 — Watch the sensor fire (if drift detected)
In Dagster UI → Automation → Sensors → `drift_retrain_sensor`:
- Click **Evaluate** to manually tick the sensor
- If drift_summary shows `any_drift=True` → you'll see run requests created
- Dagster UI → Runs → should show new `detector_retrain_job` and/or
  `forecaster_retrain_job` runs queued

### 6 — Verify cooldown is respected
After a successful trigger, the sensor should skip on the next tick:
```bash
ls .retrain_cooldowns/
# Should show: detector_retrain.last_triggered
#              forecaster_retrain.last_triggered
```

Manually evaluate the sensor again in the UI — it should return a
SkipReason mentioning "cooldown".

### 7 — Test the full closed loop end-to-end
```bash
# 1. Run generator in fast mode for 2+ minutes
python -m generator.producer --mode fast

# 2. Run drift job
python -m observability.drift_job

# 3. If drift detected, check Dagster runs
# 4. After retrain completes, check MLflow for new model version
#    http://localhost:5000 → Models → veloshelf-anomaly-detector
# 5. Within 5 min, Flink job hot-swaps to new model
#    docker-compose logs flink-taskmanager | grep "Hot-swapped"
```

---

## Design notes (for interviews)

**Why a Dagster sensor rather than a cron that always retrains?**
Retrain only when the data distribution has actually shifted — not on
a fixed schedule. This is more cost-efficient and avoids unnecessary
model churn. The drift signal is the trigger, not the clock.

**Why separate cooldown from the MLflow promotion gate?**
Two independent guardrails serve different failure modes:
- Promotion gate (Phase 3): "is the new model actually better?"
  Guards against promoting a worse model.
- Cooldown (Phase 5):       "are we retraining too frequently?"
  Guards against retrain storms from a noisy or persistent drift signal.
Both are needed. A model could be better than the incumbent but still
trigger too frequently if drift is persistent.

**Why does the sensor fall back to running drift_job inline?**
If the Dagster materialization metadata doesn't carry the drift summary
(e.g. the drift job ran outside Dagster), the sensor runs the drift job
directly to get fresh data. This makes the sensor more robust to
out-of-band drift runs.

**What happens during the hot-swap?**
After the new model is promoted in the MLflow registry, the Flink
`FeatureSinkFn`'s `HotSwapModelLoader` picks it up within
MODEL_POLL_INTERVAL_S (default 300s / 5 min) without a job restart.
The old model continues scoring during the gap — there is no downtime.

**Full loop timing (approximate):**
- drift_check_schedule fires (every 2h)
- drift_job runs: ~10–30s
- sensor evaluates (every 5min): picks up new drift_report
- if drift: retrain jobs queue immediately
- detector training: ~1–3 min (depends on data size)
- promotion gate: ~10s
- Flink hot-swap: within 5 min of promotion
- Total lag: drift detection → new model live ≈ 10–15 min

---

## Deferred to Phase 6
- Terraform IaC for AWS deployment
- GitHub Actions CI/CD (OIDC)
- Switch FEATURES_PATH to S3
- EKS manifests (design intent, not run)
- README polish with architecture diagram + screenshots