# Predictive Maintenance Pipeline for Factory Equipment
### Complete Build Guide (Azure IoT + Data Engineering)

---

## 1. Project Overview

**What it is:** A real-time + batch data pipeline that ingests simulated factory machine telemetry, detects anomalies before failure, and surfaces machine health through a dashboard.

**Why you're building it (your honest narrative — use this in interviews and the README):**
> "Unplanned downtime is one of the costliest problems in manufacturing. I wanted hands-on experience with the Azure data engineering stack in a realistic end-to-end scenario, so I built a pipeline that ingests machine sensor data, flags anomalies in real time, and rolls up daily reliability metrics for a dashboard a plant manager could actually use."

Don't claim manufacturing domain experience you don't have — this framing is honest: you built it to learn the stack in a realistic scenario, not because you've worked in a factory.

---

## 2. Architecture

```
Simulated machines (Python, 5-10 virtual "machines")
  emits: machine_id, timestamp, temperature, vibration, rpm, status

        │
        ▼
Azure IoT Hub  (device-to-cloud ingestion, per-device identity)
        │
        ▼
Azure Stream Analytics
  - 5-min tumbling window: avg/max temperature & vibration per machine
  - anomaly rule: vibration > threshold OR rapid rate-of-change
  - outputs to TWO sinks:
        │
   ┌────┴─────┐
   ▼          ▼
Azure Data   Azure SQL Database
Lake Gen2    (hot path: latest status + active alerts table)
(raw, Parquet,
partitioned by
date/machine_id)
   │
   ▼
Azure Databricks (batch, scheduled)
  - reads raw Parquet
  - dedup, backfill
  - computes daily MTBF-style reliability metric
  - writes curated/gold Delta tables
   │
   ▼
Azure Data Factory
  - orchestrates the Databricks job nightly
  - validates schema on new files landing in raw zone
   │
   ▼
Power BI
  - machine status grid, alert feed, daily downtime-risk trend
```

---

## 3. Prerequisites

- Azure subscription (free tier is enough to start; Databricks needs pay-as-you-go)
- Resources to create: IoT Hub, Storage Account (ADLS Gen2 enabled), Azure SQL Database, Stream Analytics job, Azure Databricks workspace, Azure Data Factory, Power BI Desktop (free)
- Local: Python 3.10+, `azure-iot-device` SDK, VS Code or similar

---

## 4. Step-by-Step Build

### Step 1 — Sensor Simulator (Python)

Create `simulator/sensor_simulator.py`:

```python
import time
import json
import random
import uuid
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, Message

# One connection string per simulated device (register 5-10 devices in IoT Hub)
DEVICE_CONNECTION_STRINGS = [
    "HostName=<your-hub>.azure-devices.net;DeviceId=machine-01;SharedAccessKey=<key1>",
    "HostName=<your-hub>.azure-devices.net;DeviceId=machine-02;SharedAccessKey=<key2>",
    # add more machines...
]

def generate_reading(machine_id: str, inject_anomaly: bool = False, inject_bad_schema: bool = False):
    base_temp = random.uniform(60, 75)
    base_vibration = random.uniform(0.2, 0.8)

    if inject_anomaly:
        base_temp += random.uniform(15, 30)
        base_vibration += random.uniform(1.5, 3.0)

    reading = {
        "machine_id": machine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(base_temp, 2),
        "vibration": round(base_vibration, 3),
        "rpm": random.randint(1200, 1800),
        "status": "running" if not inject_anomaly else "warning",
    }

    if inject_bad_schema:
        # simulate a malformed/missing-field event to test ADF schema validation later
        del reading["vibration"]

    return reading

def run_device(connection_string: str, machine_id: str):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    client.connect()
    print(f"[{machine_id}] connected")

    tick = 0
    try:
        while True:
            tick += 1
            anomaly = random.random() < 0.03       # ~3% chance of anomaly
            bad_schema = random.random() < 0.01     # ~1% chance of malformed event

            reading = generate_reading(machine_id, inject_anomaly=anomaly, inject_bad_schema=bad_schema)
            msg = Message(json.dumps(reading))
            msg.content_encoding = "utf-8"
            msg.content_type = "application/json"
            client.send_message(msg)
            print(f"[{machine_id}] sent: {reading}")

            time.sleep(random.uniform(2, 5))
    except KeyboardInterrupt:
        client.disconnect()

if __name__ == "__main__":
    import threading
    threads = []
    for i, conn_str in enumerate(DEVICE_CONNECTION_STRINGS, start=1):
        machine_id = f"machine-{i:02d}"
        t = threading.Thread(target=run_device, args=(conn_str, machine_id), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
```

**Azure setup for this step:**
```bash
az iot hub create --name <your-hub> --resource-group <rg> --sku S1
az iot hub device-identity create --hub-name <your-hub> --device-id machine-01
az iot hub device-identity connection-string show --hub-name <your-hub> --device-id machine-01
```
Repeat device-identity creation for each simulated machine, and paste the connection strings into the script.

---

### Step 2 — Azure Stream Analytics Job

Input: IoT Hub (consumer group `streamanalytics`)
Outputs: two — one to ADLS Gen2 (raw path), one to Azure SQL (hot path/alerts)

**Stream Analytics Query (SAQL):**

```sql
-- Raw pass-through to Data Lake (partitioned by date via output path config)
SELECT
    machine_id,
    EventEnqueuedUtcTime AS event_time,
    temperature,
    vibration,
    rpm,
    status
INTO
    [datalake-output]
FROM
    [iothub-input]

-- Windowed aggregation + anomaly detection to SQL hot path
SELECT
    machine_id,
    System.Timestamp() AS window_end,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp,
    AVG(vibration) AS avg_vibration,
    MAX(vibration) AS max_vibration,
    CASE
        WHEN MAX(vibration) > 2.0 OR MAX(temperature) > 95 THEN 'ALERT'
        ELSE 'OK'
    END AS health_status
INTO
    [sql-output]
FROM
    [iothub-input]
GROUP BY
    machine_id,
    TumblingWindow(minute, 5)
```

**Note on late-arriving events (real friction point to document in your README):**
Add event ordering settings in the Stream Analytics job: set "Out of order events" tolerance (e.g., 5 seconds) and drop/adjust as appropriate. Document what you observed when a device's clock drifted or a message arrived late — this is a genuine engineering decision, not boilerplate.

---

### Step 3 — Azure Data Lake Storage Gen2

Create containers:
- `raw/` — Parquet output from Stream Analytics, partitioned by `year/month/day/machine_id`
- `curated/` — Delta tables written by Databricks (gold layer)

```bash
az storage account create --name <yourstorageacct> --resource-group <rg> \
    --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true

az storage container create --account-name <yourstorageacct> --name raw
az storage container create --account-name <yourstorageacct> --name curated
```

---

### Step 4 — Azure SQL Database (hot path)

```sql
CREATE TABLE machine_status (
    machine_id VARCHAR(20),
    window_end DATETIME2,
    avg_temp FLOAT,
    max_temp FLOAT,
    avg_vibration FLOAT,
    max_vibration FLOAT,
    health_status VARCHAR(10)
);

CREATE TABLE active_alerts (
    alert_id INT IDENTITY PRIMARY KEY,
    machine_id VARCHAR(20),
    triggered_at DATETIME2,
    reason VARCHAR(200),
    resolved BIT DEFAULT 0
);
```

---

### Step 5 — Azure Databricks (batch/gold layer)

Create `databricks/daily_reliability_job.py`:

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

RAW_PATH = "abfss://raw@<yourstorageacct>.dfs.core.windows.net/"
CURATED_PATH = "abfss://curated@<yourstorageacct>.dfs.core.windows.net/gold_reliability"

# Read raw parquet landed by Stream Analytics
raw_df = spark.read.parquet(RAW_PATH)

# Dedup: keep latest reading per machine per minute (handles late/duplicate IoT Hub deliveries)
dedup_df = (
    raw_df
    .withColumn("minute_bucket", F.date_trunc("minute", "event_time"))
    .withColumn("row_num", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window
        .partitionBy("machine_id", "minute_bucket")
        .orderBy(F.col("event_time").desc())
    ))
    .filter("row_num = 1")
    .drop("row_num")
)

# Daily reliability metric: count of ALERT-flagged windows and estimated MTBF proxy
daily_metrics = (
    dedup_df
    .withColumn("date", F.to_date("event_time"))
    .withColumn("is_anomaly", (F.col("temperature") > 90) | (F.col("vibration") > 1.8))
    .groupBy("machine_id", "date")
    .agg(
        F.count("*").alias("total_readings"),
        F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
        F.avg("temperature").alias("avg_daily_temp"),
        F.avg("vibration").alias("avg_daily_vibration"),
    )
    .withColumn("uptime_score", 1 - (F.col("anomaly_count") / F.col("total_readings")))
)

# Write/merge into gold Delta table
if DeltaTable.isDeltaTable(spark, CURATED_PATH):
    gold = DeltaTable.forPath(spark, CURATED_PATH)
    (
        gold.alias("t")
        .merge(daily_metrics.alias("s"), "t.machine_id = s.machine_id AND t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    daily_metrics.write.format("delta").save(CURATED_PATH)

print("Daily reliability metrics updated.")
```

**Cluster note:** use a small single-node cluster (e.g., Standard_DS3_v2) for cost control — worth mentioning in your README that you deliberately right-sized compute for a low-volume workload.

---

### Step 6 — Azure Data Factory (orchestration)

Pipeline `pl_daily_reliability`:
1. **Get Metadata** activity — check new files landed in `raw/` since last run
2. **If Condition** — if no new files, skip run (log to a pipeline run log table)
3. **Validation** activity — lightweight schema check (e.g., a Databricks notebook or ADF Data Flow that flags rows missing required fields, like the `bad_schema` events your simulator occasionally sends)
4. **Databricks Notebook** activity — triggers `daily_reliability_job.py`
5. **Trigger:** scheduled nightly (e.g., 2 AM) via a Tumbling Window or Schedule trigger

Document in your README: what happened when the malformed events (missing `vibration` field) hit the validation step — did they get quarantined, logged, or dropped? This is your "requirements-driven design" evidence: you decided bad records shouldn't silently break the pipeline, so you built a validation gate.

---

### Step 7 — Power BI Dashboard

Connect Power BI to:
- Azure SQL (`machine_status`, `active_alerts`) for the live/hot view
- ADLS Gen2 gold Delta table (via Databricks SQL endpoint or direct connector) for daily trends

Build:
- **Machine status grid** — one tile per machine, color-coded by `health_status`
- **Alert feed** — table of `active_alerts`, sorted by most recent
- **Daily downtime-risk trend** — line chart of `uptime_score` per machine over time, from the gold table

---

## 5. README Template (put this in your repo)

```markdown
# Predictive Maintenance Pipeline for Factory Equipment

## Why I built this
[Use the honest narrative from Section 1 above — rewrite in your own words]

## Architecture
[Paste the diagram from Section 2]

## Design decisions & requirements reasoning
- Chose a 5-minute tumbling window because [your reasoning — e.g., balance between
  responsiveness and noise reduction]
- Dual-sink Stream Analytics output (SQL for hot path, Data Lake for historical) because
  [operational alerts need low latency; historical analysis needs full-fidelity raw data]
- Added a schema validation gate in ADF after observing malformed events from the simulator,
  to avoid silently corrupting downstream aggregates

## What I'd improve with more time
- Replace fixed anomaly thresholds with a rolling z-score or a simple trained model
- Add device-level configuration (different thresholds per machine type)
- Add integration tests for the Databricks job

## Tech stack
Azure IoT Hub, Azure Stream Analytics, Azure Data Lake Storage Gen2, Azure Databricks
(PySpark + Delta Lake), Azure SQL Database, Azure Data Factory, Power BI
```

---

## 6. Two-Week Build Schedule

| Days | Task |
|---|---|
| 1-2 | Sensor simulator + IoT Hub, confirm messages flowing (use Azure IoT Explorer to verify) |
| 3-5 | Stream Analytics job: windowing, anomaly rule, dual output |
| 6-8 | Databricks batch job: dedup, gold Delta table |
| 9-10 | Data Factory pipeline: orchestration + schema validation |
| 11-12 | Power BI dashboard |
| 13-14 | README, architecture diagram, honest "what I'd improve" section |

---

## 7. Interview Talking Points (prep these, don't memorize word-for-word)

- Why IoT Hub over raw MQTT/REST ingestion (device identity, built-in retry/backoff, per-device auth)
- Why two Stream Analytics outputs instead of one (different latency/consumption needs for alerts vs. history)
- What the late-arriving-event tolerance setting does and what you observed
- Why Delta Lake merge instead of overwrite for the gold table (idempotent reruns)
- What you'd do differently at real production scale (partitioning strategy, cost of Databricks always-on clusters, using Azure Monitor for pipeline observability)

Be ready to say plainly: this is a self-directed learning project, not professional experience — and that's fine. It shows initiative and gives you a concrete, defensible story instead of a resume line with no substance behind it.
