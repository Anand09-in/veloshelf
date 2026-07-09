# Phase 1 — Synthetic Event Generator

A Python Kafka producer that emits realistic order and inventory events for a quick-commerce dark store network. Arrival rates follow a time-varying Poisson process; SKU popularity follows a Zipf (long-tail) distribution; anomalies are injected deliberately with ground-truth labels.

---

## Entry point

```bash
python -m generator.producer --mode fast
```

`--mode fast` compresses time (events arrive ~100× faster than wall-clock) to fill windowed features quickly for development and demos. Without `--mode`, events arrive at realistic Poisson rates (~2–5 orders/min per store).

---

## Files

| File | Role |
|---|---|
| `generator/producer.py` | Main loop: generates events, calls injector, serialises to JSON, produces to Kafka |
| `generator/distributions.py` | Poisson arrival model, Zipf SKU popularity, time-of-day + weekend multipliers |
| `generator/anomaly_injector.py` | Decides when to inject anomalies, writes labels to `data/seeds/anomaly_labels.jsonl` |
| `generator/schemas.py` | Pydantic models: `OrderEvent`, `InventoryMovementEvent` |

---

## Arrival model — `distributions.py`

**Order arrivals** are sampled from a Poisson process with a time-varying rate:

```
λ(t) = λ_base × time_of_day_multiplier(t) × weekend_multiplier(t)
```

- `λ_base` ≈ 3 orders/min per store (configurable)
- `time_of_day_multiplier`: peaks at lunch (1.4×) and evening (1.6×), troughs at night (0.3×)
- `weekend_multiplier`: 1.3× on Saturday/Sunday

**SKU selection** follows a truncated Zipf distribution over 500 SKUs. The most popular SKU gets ~8% of demand; the top 20 SKUs account for ~60%; the long tail (bottom 400) shares the rest. This mirrors real quick-commerce demand concentration.

**Basket size** (units per order) is drawn from a discrete distribution skewed toward 1–3 units, with rare large baskets (up to 12 units) for pantry-loading events.

**Inventory movements** are emitted alongside each order: `delta_units = -quantity`, `on_hand_after` decrements from an in-memory per-SKU stock level. Restock events fire probabilistically when `on_hand_after` drops below the SKU's `reorder_point`.

---

## Anomaly injection — `anomaly_injector.py`

Two anomaly types are injected:

**Demand surge** — a specific SKU's order rate multiplies by 5–10× for 2–5 minutes. Models a real event (rain, social media mention, competitor stockout). The surge is implemented by overriding the Zipf weight for that SKU during the injection window.

**Accelerated depletion** — `delta_units` is set to a larger negative value than the order quantity, simulating spoilage, theft, or miscounted stock. `on_hand_after` drops faster than orders justify.

Every injected event sets `is_injected_anomaly: true` on the event and appends a record to `data/seeds/anomaly_labels.jsonl`:

```json
{"event_id": "...", "sku_id": "SKU_042", "store_id": "DS_001",
 "anomaly_type": "demand_surge", "injected_at": "2024-01-15T14:32:11Z"}
```

This file is the ground-truth reference for `ml/evaluate.py` — it joins model predictions against known injection timestamps to compute real precision and recall. This is the key advantage of synthetic data: labelled ground truth is free.

---

## Kafka producer

`producer.py` creates a `KafkaProducer` with:
- `bootstrap_servers` from `KAFKA_BOOTSTRAP_SERVERS` env var (`kafka:29092` inside Docker, `localhost:9092` from host)
- `value_serializer`: JSON encode + UTF-8
- `key_serializer`: `store_id` as bytes (ensures events for the same store land on the same partition)

Events are published to two topics:
- `raw-orders` — `OrderEvent` JSON
- `raw-inventory` — `InventoryMovementEvent` JSON

The producer runs in a tight loop. In `--mode fast`, it sleeps 0 ms between events; in normal mode it sleeps to match the Poisson inter-arrival time.

---

## Kafka topics

All topics have 3 partitions and replication factor 1 (single-broker). Partitioning is by `store_id` — all events for a given store go to the same partition, preserving per-store event ordering for the Flink window operator.

| Topic | Publisher | Consumer |
|---|---|---|
| `raw-orders` | generator | Flink job |
| `raw-inventory` | generator | Flink job |
| `dead-letter` | Flink (invalid events) | manual inspection |
| `stockout-alerts` | Flink scorer | downstream consumers |
| `surge-alerts` | Flink scorer | downstream consumers |
