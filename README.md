# Predictive Maintenance Pipeline for Factory Equipment

## Why I built this

Unplanned downtime is one of the costliest problems in manufacturing. I wanted hands-on
experience with the Azure data engineering stack in a realistic end-to-end scenario, so I
built a pipeline that ingests machine sensor data, flags anomalies in real time, and rolls
up daily reliability metrics for a dashboard a plant manager could actually use.

I don't have manufacturing domain experience — this is a self-directed learning project, not
professional experience, and that's fine. It shows initiative and gives a concrete, defensible
story instead of a resume line with no substance behind it.

This repo has two parallel implementations of the same architecture:

- **`local_sim/`** — runs entirely on your machine, no Azure account or cost required. Every
  component (ingestion, stream processing, batch job, orchestration) is a real, working Python
  implementation of the same logic the Azure services would run, using local files/SQLite
  instead of managed services.
- **`azure/`** — the actual Azure artifacts (IoT Hub device SDK script, Stream Analytics query,
  SQL DDL, Databricks PySpark job) ready to deploy once you have a subscription. See
  `predictive-maintenance-pipeline-guide.md` in the repo root for the full deployment walkthrough
  (`az` CLI commands, resource provisioning, Power BI setup).

## Architecture

```
Simulated machines (Python, 5-10 virtual "machines")
  emits: machine_id, timestamp, temperature, vibration, rpm, status

        │
        ▼
IoT Hub  (device-to-cloud ingestion, per-device identity)
  Azure: Azure IoT Hub   |   local_sim: ingestion/iot_hub_emulator.py
        │
        ▼
Stream Analytics
  - 5-min tumbling window: avg/max temperature & vibration per machine
  - anomaly rule: vibration > threshold OR rapid rate-of-change
  - outputs to TWO sinks:
  Azure: Stream Analytics job   |   local_sim: stream_processor/stream_analytics_emulator.py
        │
   ┌────┴─────┐
   ▼          ▼
Data Lake    SQL Database
(raw,        (hot path: latest status + active alerts)
Parquet,     Azure: Azure SQL   |   local_sim: SQLite (storage/hot_path/)
partitioned
by date/
machine_id)
Azure: ADLS Gen2   |   local_sim: storage/raw/ (Parquet)
   │
   ▼
Databricks (batch, scheduled)
  - reads raw Parquet
  - dedup, backfill
  - computes daily MTBF-style reliability metric
  - writes curated/gold Delta tables
  Azure: Azure Databricks (PySpark + Delta)   |   local_sim: batch/daily_reliability_job.py (pandas)
   │
   ▼
Data Factory
  - orchestrates the Databricks job nightly
  - validates schema on new files landing in raw zone (quarantines malformed events)
  Azure: Azure Data Factory   |   local_sim: orchestration/orchestrator.py
   │
   ▼
Dashboard
  - machine status grid, alert feed, daily downtime-risk trend
  Azure: Power BI   |   local_sim: dashboard/app.py (Streamlit)
```

## Quickstart (local simulation)

```bash
pip install -r requirements.txt

# 1. Generate telemetry (5 virtual machines, 90 seconds, ~1.5s between readings)
python local_sim/simulator/sensor_simulator.py --machines 5 --duration 90

# 2. Process it into the raw data lake + hot-path SQLite (5-min tumbling windows)
python local_sim/stream_processor/stream_analytics_emulator.py --once

# 3. Run the orchestrated batch job (schema validation + dedup + daily reliability metrics)
python local_sim/orchestration/orchestrator.py

# 4. View the dashboard
streamlit run dashboard/app.py
```

Notes on step 2: Stream Analytics uses real 5-minute tumbling windows, so a window only
"closes" (and appears in the hot-path/dashboard) once 5 minutes plus the late-arrival
tolerance have passed. For a live demo, run the simulator with `--duration 0` (runs until
Ctrl+C) and re-run the stream processor with `--watch` in a second terminal so windows close
naturally as time passes:

```bash
python local_sim/simulator/sensor_simulator.py --machines 5 --duration 0
python local_sim/stream_processor/stream_analytics_emulator.py --watch --poll 5
```

Re-run `orchestrator.py` any time after new raw files have landed — it's idempotent
(safe to re-run; it skips if nothing new landed, and merges/upserts into the gold table
rather than duplicating rows).

## Design decisions & requirements reasoning

- **5-minute tumbling window**: balances alert responsiveness against noise — shorter windows
  would flag every brief vibration spike; this smooths transient blips while still catching
  sustained problems within a few minutes.
- **Dual-sink Stream Analytics output** (SQL hot path + Data Lake raw): operational alerts
  need low latency and only care about "right now"; historical/trend analysis needs the
  full-fidelity raw record. One sink can't serve both well.
- **Late-arriving events**: a real Stream Analytics job needs an explicit out-of-order
  tolerance, or a window "closes" before all its events have arrived and your aggregate is
  wrong. `stream_analytics_emulator.py` holds back the current (still-open) window and only
  finalizes windows past a tolerance cutoff — the same idea, applied locally.
- **Schema validation deferred to orchestration, not streaming**: the simulator occasionally
  drops the `vibration` field to mimic a malformed device payload. Rather than have the stream
  processor silently drop or crash on these, they're written through to raw storage as-is (null
  vibration) and caught by the orchestrator's validation gate, which quarantines them before
  they can corrupt the daily aggregates. This mirrors putting a validation activity in ADF
  rather than baking it into the streaming query — it's explicit, inspectable, and testable in
  isolation.
- **Delta-style merge instead of overwrite for the gold table**: re-running the batch job (e.g.
  after fixing a bug, or backfilling a day) should be idempotent — it should replace that day's
  numbers, not duplicate them or wipe every other day's history.
- **Small single-node compute for the batch job**: the real Databricks job note in the guide
  calls out right-sizing a cluster (Standard_DS3_v2) for a low-volume workload — worth
  mentioning in interviews that cost-consciousness is a design decision, not an afterthought.

## What I'd improve with more time

- Replace fixed anomaly thresholds with a rolling z-score or a simple trained model
- Add device-level configuration (different thresholds per machine type)
- Add integration tests for the batch job and stream processor
- Add a proper consumer-group-style checkpoint (currently a single byte offset) if this needed
  to support multiple independent downstream readers
- Package the local simulation as Docker Compose so all four processes run with one command

## Tech stack

**Local simulation:** Python, pandas, pyarrow (Parquet), SQLite, Streamlit

**Real Azure deployment (see `azure/` and `predictive-maintenance-pipeline-guide.md`):**
Azure IoT Hub, Azure Stream Analytics, Azure Data Lake Storage Gen2, Azure Databricks
(PySpark + Delta Lake), Azure SQL Database, Azure Data Factory, Power BI

## Repo structure

```
predictive-maintenance-pipeline-guide.md   # original step-by-step Azure build guide
README.md                                  # this file
requirements.txt

local_sim/                    # runnable now, no Azure account needed
  simulator/sensor_simulator.py
  ingestion/iot_hub_emulator.py
  stream_processor/stream_analytics_emulator.py
  batch/daily_reliability_job.py
  orchestration/orchestrator.py
  storage/                    # raw/, curated/, hot_path/, quarantine/, checkpoints/ (gitignored contents)
  logs/pipeline_runs.jsonl

dashboard/app.py              # Streamlit dashboard (Power BI emulation)

azure/                        # real Azure artifacts, ready to deploy
  simulator/sensor_simulator_azure.py
  stream_analytics/query.saql
  sql/schema.sql
  databricks/daily_reliability_job.py
  adf/pipeline_notes.md
```

## Interview talking points (prep these, don't memorize word-for-word)

- Why IoT Hub over raw MQTT/REST ingestion (device identity, built-in retry/backoff, per-device auth)
- Why two Stream Analytics outputs instead of one (different latency/consumption needs for alerts vs. history)
- What the late-arriving-event tolerance setting does and what you observed building the local emulator
- Why a Delta-style merge instead of overwrite for the gold table (idempotent reruns)
- Why schema validation lives in the orchestration layer, not the streaming layer
- What you'd do differently at real production scale (partitioning strategy, cost of
  Databricks always-on clusters, using Azure Monitor for pipeline observability)

Be ready to say plainly: this is a self-directed learning project, not professional
experience — and that's fine.
