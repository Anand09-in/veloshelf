# Phase 2 — Stream Processing (PyFlink)

A PyFlink 1.18 pipeline that reads raw order and inventory events from Kafka, validates them, computes event-time windowed features, scores for stockout and surge risk, and writes results to Postgres and alert Kafka topics.

---

## Files

| File | Role |
|---|---|
| `streaming/job.py` | PyFlink pipeline — sources, window operator, scoring, sinks |
| `streaming/validation.py` | Schema + range checks; dead-letter envelope builder |
| `streaming/scoring.py` | Stockout + surge rule scorer; ML hot-swap integration |
| `streaming/sinks.py` | `PostgresSink` (upsert features + insert alerts); `KafkaAlertSink` |
| `infra/init_db.sql` | DDL for `windowed_features` + `alerts` tables |
| `Dockerfile.flink` | Flink 1.18 image with Python 3.11 and the Kafka SQL connector JAR |

---

## Pipeline — `streaming/job.py`

The job uses the **PyFlink Table API** (not DataStream). This gives clean SQL-style window syntax and integrates natively with the Flink planner's watermark handling.

### Sources

Two Kafka sources are registered as catalog tables:
- `raw_orders` — reads from `raw-orders`, deserialises JSON, applies watermark on `event_time` with a 10s allowed lateness
- `raw_inventory` — same pattern on `raw-inventory`

`WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND` tells Flink how late events can arrive before a window closes.

### Validation

Every event passes through `streaming/validation.py` before windowing. Checks:
- Required fields present and correct type
- `quantity` and `delta_units` within plausible ranges (−100 to 100)
- `store_id` and `sku_id` in the known dimension sets
- `event_time` is a parseable ISO-8601 timestamp

Invalid events are wrapped in a dead-letter envelope `{original_payload, error_reason, failed_at}` and published to the `dead-letter` Kafka topic. They do not enter the window operator.

### Windowed feature computation

A 1-minute **tumbling event-time window** over the combined order + inventory stream produces one feature row per (store_id, sku_id, window):

| Feature | Derivation |
|---|---|
| `order_rate` | COUNT(orders) / window_duration_minutes |
| `depletion_velocity` | SUM(ABS(delta_units)) / window_duration_minutes |
| `demand_momentum` | order_rate / 5-min rolling average order_rate |
| `avg_basket_size` | AVG(quantity) over order events in window |
| `on_hand_est` | LAST(on_hand_after) from inventory stream |
| `volume_imbalance` | (orders_in − restock_units) / total_units |

`demand_momentum > 1` means this window's rate exceeds the recent baseline — the primary surge signal.

### Scoring — `streaming/scoring.py`

After windowing, each feature row is passed to the scorer. The scorer starts rule-based and hot-swaps to the ML model once one is promoted to MLflow `Production` (see Phase 3).

**Rule-based fallback:**
- Stockout: `on_hand_est < reorder_point` (from `dim_sku`) **and** `depletion_velocity > threshold`
- Surge: `demand_momentum > 2.5`

**ML scoring:** `HotSwapModelLoader` (from `ml/model_loader.py`) polls the MLflow registry every 5 minutes. When a `Production` model exists, it replaces the rule-based scorer. The old model continues scoring during the transition — no downtime.

Alerts are `{alert_type, store_id, sku_id, severity, score, details}` dicts.

### Sinks — `streaming/sinks.py`

**`PostgresSink`** — for each window close, upserts the feature row into `windowed_features` using `ON CONFLICT (store_id, sku_id, window_start) DO UPDATE SET ...`. Alerts are inserted into the `alerts` table. Uses `psycopg` (the v3 sync API) with a connection per task.

**`KafkaAlertSink`** — publishes stockout alerts to `stockout-alerts` and surge alerts to `surge-alerts` as JSON. Downstream consumers (Streamlit, Grafana) can subscribe independently.

---

## Kafka connector

`Dockerfile.flink` downloads `flink-sql-connector-kafka-3.1.0-1.18.jar` into `/opt/flink/lib/`. This is the only JAR dependency — Flink's built-in Table API handles everything else.

The job is submitted with:
```bash
PYFLINK_PYTHON=python3 flink run -py /opt/veloshelf/streaming/job.py --detached
```

`--detached` returns immediately; the job runs on the cluster. Check status at http://localhost:8081 → Running Jobs.

---

## Dead-letter quarantine

Events that fail validation are published to `dead-letter` with the reason embedded. They are:
- **Not counted** in window metrics (no silent data corruption)
- **Replayable** — the original payload is preserved, so the event can be corrected and re-submitted
- **Visible** — the topic can be inspected with `kafka-console-consumer.sh` or Kafka UI

This is a maturity signal most demo pipelines skip. It demonstrates that the pipeline treats bad data as a first-class concern rather than silently dropping it.

---

## Design decisions

**Why Table API over DataStream API?**
Table API windowing is declarative — `TUMBLE(TABLE t, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)` makes the intent obvious. DataStream would require `KeyedStream.window(TumblingEventTimeWindows.of(...))` boilerplate with manual `WindowFunction` implementations for the same result.

**Why 1-min tumbling (not sliding)?**
1-min tumbling windows give a clean, non-overlapping per-window snapshot: one `order_rate` value, one `depletion_velocity`. `demand_momentum` is computed as the ratio of the current window rate to the 5-min rolling average, which is tracked as a running state rather than a second window operator — simpler and avoids the memory overhead of overlapping windows.

**Why event-time (not processing-time)?**
Processing-time windows are non-deterministic — reprocessing the same event stream would produce different window boundaries depending on when events arrive at the operator. Event-time ensures that a window covers exactly the same events regardless of pipeline lag or replay. The 10-second allowed lateness handles Kafka consumer lag and minor clock skew.
