"""
stream_analytics_emulator.py

Local stand-in for the Azure Stream Analytics job described in the build guide
(section "Step 2 - Azure Stream Analytics Job"). Reads new messages from the
iot_hub_emulator queue and fans them out to two sinks, mirroring the two SAQL
SELECT...INTO statements in the guide:

  1. Raw pass-through -> Parquet, partitioned by date/machine_id (data lake "raw" zone)
  2. 5-minute tumbling window aggregation + anomaly rule -> SQLite hot path
     (machine_status + active_alerts tables)

Design notes carried over from the guide (documented here instead of a README so
they stay next to the code that implements them):

  - Late-arriving events: real Stream Analytics jobs need an explicit "out of order
    events" tolerance. Here we hold back the most recent (still-open) 5-minute
    window on each run and only aggregate windows that have fully closed, which is
    the same idea - don't finalize a window until you're reasonably sure no more
    events for it are coming.
  - Malformed events (missing `vibration`, from the simulator's bad_schema
    injection) are written through to raw storage as-is (with a null vibration)
    rather than dropped - schema validation is deliberately deferred to the
    orchestration step (see orchestration/orchestrator.py), matching the real
    ADF validation-gate design in the guide.

Usage:
    python stream_analytics_emulator.py --once        # process everything available now
    python stream_analytics_emulator.py --watch --poll 5   # keep polling every 5s
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.iot_hub_emulator import read_new_messages

BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / "storage"
RAW_DIR = STORAGE_DIR / "raw"
HOT_PATH_DB = STORAGE_DIR / "hot_path" / "hot_path.db"
CHECKPOINT_FILE = STORAGE_DIR / "checkpoints" / "stream_checkpoint.json"

ANOMALY_VIBRATION_THRESHOLD = 2.0
ANOMALY_TEMP_THRESHOLD = 95
LATE_ARRIVAL_TOLERANCE_SECONDS = 5  # mirrors the ASA "out of order events" setting


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"queue_offset": 0}


def save_checkpoint(cp):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps(cp))


def ensure_hot_path_schema():
    HOT_PATH_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(HOT_PATH_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS machine_status (
            machine_id TEXT,
            window_end TEXT,
            avg_temp REAL,
            max_temp REAL,
            avg_vibration REAL,
            max_vibration REAL,
            health_status TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS active_alerts (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT,
            triggered_at TEXT,
            reason TEXT,
            resolved INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def messages_to_dataframe(messages):
    rows = []
    for m in messages:
        body = m.get("body", {})
        rows.append({
            "machine_id": body.get("machine_id") or m.get("device_id"),
            "event_time": body.get("timestamp") or m.get("enqueued_time"),
            "temperature": body.get("temperature"),
            "vibration": body.get("vibration"),  # may be missing -> NaN, matches ASA passing nulls through
            "rpm": body.get("rpm"),
            "status": body.get("status"),
        })
    if not rows:
        return pd.DataFrame(columns=["machine_id", "event_time", "temperature", "vibration", "rpm", "status"])
    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    return df


def write_raw_sink(df: pd.DataFrame):
    """Partition by date/machine_id, one small parquet file per run (append-style, like ASA blob output)."""
    if df.empty:
        return 0
    written = 0
    for (date, machine_id), group in df.groupby([df["event_time"].dt.date, "machine_id"]):
        part_dir = RAW_DIR / f"date={date}" / f"machine_id={machine_id}"
        part_dir.mkdir(parents=True, exist_ok=True)
        fname = part_dir / f"part-{int(time.time()*1000)}.parquet"
        group.drop(columns=[]).to_parquet(fname, index=False)
        written += len(group)
    return written


def compute_tumbling_windows(df: pd.DataFrame, now_utc):
    """5-minute tumbling window aggregation, holding back the still-open current window."""
    if df.empty:
        return pd.DataFrame()

    valid = df.dropna(subset=["event_time"]).copy()
    valid["window_end"] = valid["event_time"].dt.ceil("5min")

    cutoff = now_utc - pd.Timedelta(seconds=LATE_ARRIVAL_TOLERANCE_SECONDS)
    closed = valid[valid["window_end"] <= cutoff]
    if closed.empty:
        return pd.DataFrame()

    agg = (
        closed.groupby(["machine_id", "window_end"])
        .agg(
            avg_temp=("temperature", "mean"),
            max_temp=("temperature", "max"),
            avg_vibration=("vibration", "mean"),
            max_vibration=("vibration", "max"),
        )
        .reset_index()
    )
    agg["health_status"] = agg.apply(
        lambda r: "ALERT" if (pd.notna(r["max_vibration"]) and r["max_vibration"] > ANOMALY_VIBRATION_THRESHOLD)
        or (pd.notna(r["max_temp"]) and r["max_temp"] > ANOMALY_TEMP_THRESHOLD)
        else "OK",
        axis=1,
    )
    return agg


def write_hot_path_sink(conn, agg: pd.DataFrame):
    if agg.empty:
        return 0, 0
    rows_written = 0
    alerts_written = 0
    for _, r in agg.iterrows():
        conn.execute(
            "INSERT INTO machine_status (machine_id, window_end, avg_temp, max_temp, avg_vibration, max_vibration, health_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (r["machine_id"], str(r["window_end"]), r["avg_temp"], r["max_temp"], r["avg_vibration"], r["max_vibration"], r["health_status"]),
        )
        rows_written += 1
        if r["health_status"] == "ALERT":
            existing = conn.execute(
                "SELECT COUNT(*) FROM active_alerts WHERE machine_id = ? AND resolved = 0", (r["machine_id"],)
            ).fetchone()[0]
            if existing == 0:
                reason = f"max_vibration={r['max_vibration']:.2f}, max_temp={r['max_temp']:.1f}"
                conn.execute(
                    "INSERT INTO active_alerts (machine_id, triggered_at, reason) VALUES (?, ?, ?)",
                    (r["machine_id"], str(r["window_end"]), reason),
                )
                alerts_written += 1
        else:
            conn.execute(
                "UPDATE active_alerts SET resolved = 1 WHERE machine_id = ? AND resolved = 0", (r["machine_id"],)
            )
    conn.commit()
    return rows_written, alerts_written


def run_once():
    cp = load_checkpoint()
    messages, new_offset = read_new_messages(cp["queue_offset"])

    if not messages:
        print("No new messages.")
        return

    df = messages_to_dataframe(messages)
    raw_written = write_raw_sink(df)

    conn = ensure_hot_path_schema()
    now_utc = pd.Timestamp.now(tz="UTC")
    agg = compute_tumbling_windows(df, now_utc)
    status_rows, alerts = write_hot_path_sink(conn, agg)
    conn.close()

    cp["queue_offset"] = new_offset
    save_checkpoint(cp)

    print(f"Processed {len(messages)} messages -> raw: {raw_written} rows written, "
          f"hot path: {status_rows} window(s), {alerts} new alert(s).")


def main():
    parser = argparse.ArgumentParser(description="Local emulator for the Azure Stream Analytics job.")
    parser.add_argument("--once", action="store_true", help="process everything currently queued, then exit")
    parser.add_argument("--watch", action="store_true", help="keep polling for new messages")
    parser.add_argument("--poll", type=float, default=5.0, help="seconds between polls in --watch mode")
    args = parser.parse_args()

    if args.watch:
        print("Watching for new telemetry... (Ctrl+C to stop)")
        try:
            while True:
                run_once()
                time.sleep(args.poll)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        run_once()


if __name__ == "__main__":
    main()
