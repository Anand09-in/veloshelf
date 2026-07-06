# Phase 2 — Stream Processing (PyFlink)

> Goal: a running PyFlink job that reads from Kafka, validates events,
> computes windowed features, scores for stockout/surge, and writes
> results to Postgres (serving store) and Kafka alert topics.

**Definition of done:**
- `make test` passes all Phase 2 tests (no Flink runtime needed).
- `make up` starts the full stack including Flink jobmanager + taskmanager.
- Flink UI reachable at http://localhost:8081.
- `make flink-submit` submits the job without errors.
- After running the generator in fast mode, `windowed_features` rows appear in Postgres.
- Alerts appear in `stockout-alerts` and `surge-alerts` topics.

---

## Task list

### New files
- [x] `streaming/__init__.py`
- [x] `streaming/validation.py`  — pure-Python validation logic, dead-letter envelope
- [x] `streaming/scoring.py`     — rule-based stockout + surge scorer, feature row builder
- [x] `streaming/sinks.py`       — PostgresSink (upsert features + insert alerts), KafkaAlertSink
- [x] `streaming/job.py`         — PyFlink pipeline (sources → validate → window → score → sink)
- [x] `infra/init_db.sql`        — Postgres DDL (windowed_features + alerts tables + indexes)
- [x] `tests/test_streaming.py`  — unit tests for validation + scoring (no Flink runtime)

### Infrastructure updates
- [x] `docker-compose.yml`       — added flink-jobmanager, flink-taskmanager, flink-jar-downloader
- [x] `Makefile`                 — added jar, initdb, topics, flink-submit targets

### Decisions locked
- [x] Serving store → **Postgres**
- [x] Flink version → **1.18** (flink:1.18-scala_2.12-java11)
- [x] Kafka connector → **flink-sql-connector-kafka 3.1.0-1.18**

### Verification steps
- [ ] `make test`        — all tests pass (smoke + phase1 + phase2)
- [ ] `make lint`        — ruff clean
- [ ] `make up`          — all services healthy including Flink
- [ ] `make jar`         — Kafka connector JAR downloaded into flink_jars volume
- [ ] `make initdb`      — windowed_features + alerts tables created in Postgres
- [ ] `make topics`      — all 5 Kafka topics exist (including stockout-alerts, surge-alerts)
- [ ] `make flink-submit` — job submitted, visible in Flink UI at http://localhost:8081
- [ ] Generator running in fast mode, features appear in Postgres
- [ ] Alerts appear in surge-alerts / stockout-alerts topics

---

## Step-by-step verification

### 1 — Tests and lint (no Flink needed)
```bash
make test
make lint
```
Expected: 20 (phase0+1) + new phase2 tests all pass.

### 2 — Start full stack
```bash
make up
docker-compose ps
```
All services should show healthy/running, including:
- `veloshelf-flink-jobmanager-1`
- `veloshelf-flink-taskmanager-1`

Flink UI: http://localhost:8081

### 3 — Download Kafka JAR (one-time)
```bash
make jar
```
This runs the `flink-jar-downloader` container and puts the JAR
into the `flink_jars` Docker volume shared with the Flink containers.

### 4 — Initialise Postgres schema
```bash
make initdb
```
Verify tables:
```bash
docker-compose exec postgres psql -U veloshelf -d veloshelf \
  -c "\dt"
```
Expected: `windowed_features` and `alerts` tables listed.

### 5 — Create all Kafka topics
```bash
make topics
```

### 6 — Submit the Flink job
```bash
make flink-submit
```
Check http://localhost:8081 → Jobs → Running Jobs. You should see
`VeloShelf Streaming Pipeline` with status RUNNING.

### 7 — Run the generator and watch features flow
In a separate terminal:
```bash
python -m generator.producer --mode fast
```
Let it run for ~90 seconds (one full 1-min tumbling window + buffer).

Then check Postgres:
```bash
docker-compose exec postgres psql -U veloshelf -d veloshelf \
  -c "SELECT store_id, sku_id, order_rate, on_hand_est, demand_momentum \
      FROM windowed_features LIMIT 10;"
```

And check alerts:
```bash
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stockout-alerts --from-beginning --max-messages 5
```

---

## Design notes (for interviews)

**Why PyFlink Table API + SQL?**
Cleaner windowing syntax, what production teams use, easier to extend.
DataStream API would mean more boilerplate for the same semantics.

**Why tumbling 1-min + sliding 5-min/1-min?**
Tumbling gives a clean per-window snapshot (order_rate, depletion_vel).
Sliding gives the longer-term baseline needed for demand_momentum
(how much faster orders are arriving vs. the recent 5-min average).

**Dead-letter quarantine — why does it matter?**
Most demo pipelines silently drop bad events. A dead-letter topic means
bad events are visible, replayable, and auditable — a basic
production-maturity signal that interviewers notice.

**Rule-based scoring now, ML scoring in Phase 3:**
The rules (on_hand < reorder_point, momentum > 2.5) give you a working
end-to-end pipeline immediately. Phase 3 replaces the rule thresholds
with the offline-trained model's predictions — the architecture doesn't
change, only the scoring function.

**Phase 2 TODO — inventory stream join:**
The order + inventory window join is currently a placeholder in job.py
(long_rate and depletion from the inventory stream are stubbed at 0/1.0).
The full interval join is the first task in Phase 2 polish, before Phase 3.

---

## PyFlink conda install note
PyFlink 1.18 requires Java 11. Install via:
```bash
conda install -c conda-forge openjdk=11 -y
pip install apache-flink==1.18.0
```
Verify:
```bash
python -c "from pyflink.datastream import StreamExecutionEnvironment; print('OK')"
```

---

## Deferred to Phase 3
- Full order + inventory stream interval join (depletion_vel from live inventory)
- Offline Spark training on S3 features
- ML model replaces rule-based scoring
- MLflow registry + hot-swap