# Local stand-in for the Azure Stream Analytics job in the build guide (Step 2).
# Pulls new messages off the iot_hub_emulator queue and fans them out to the same
# two sinks as the real SAQL query: raw pass-through to Parquet, and a 5-minute
# tumbling window aggregation (with an anomaly rule) into a SQLite hot path.
#
# Two things worth knowing if you're reading this before the README:
#
# - Windows aren't finalized until they're a few seconds past their end time
#   (LATE_ARRIVAL_TOLERANCE_SECONDS below). That's standing in for the "out of
#   order events" setting a real Stream Analytics job needs - otherwise you
#   close a window before all its events have actually arrived.
# - A message missing `vibration` (the simulator does this on purpose sometimes)
#   still gets written to raw storage as-is. Schema validation happens later, in
#   orchestrator.py - not here. Keeping the streaming path dumb and pushing
#   validation into the batch/orchestration layer means a schema check.
#
# python stream_analytics_emulator.py --once
# python stream_analytics_emulator.py --watch --poll 5

import argparse
import json
import sqlite3
import sys
import time
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
LATE_ARRIVAL_TOLERANCE_SECONDS = 5


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
            "vibration": body.get("vibration"),  # missing on bad-schema events -> NaN
            "rpm": body.get("rpm"),
            "status": body.get("status"),
        })

    if not rows:
        return pd.DataFrame(columns=["machine_id", "event_time", "temperature", "vibration", "rpm", "status"])

    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    return df


def write_raw_sink(df: pd.DataFrame) -> int:
    """One small parquet file per machine per run, partitioned by date/machine_id."""
    if df.empty:
        return 0

    # a handful of messages can fail to parse a timestamp - drop them rather than
    # write them into a nonsense "date=NaT" partition
    df = df.dropna(subset=["event_time"])
    if df.empty:
        return 0

    written = 0
    for (date, machine_id), group in df.groupby([df["event_time"].dt.date, "machine_id"]):
        part_dir = RAW_DIR / f"date={date}" / f"machine_id={machine_id}"
        part_dir.mkdir(parents=True, exist_ok=True)
        fname = part_dir / f"part-{int(time.time() * 1000)}.parquet"
        group.to_parquet(fname, index=False)
        written += len(group)
    return written


def compute_tumbling_windows(df: pd.DataFrame, now_utc) -> pd.DataFrame:
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

    def health_status(row):
        vib_alert = pd.notna(row["max_vibration"]) and row["max_vibration"] > ANOMALY_VIBRATION_THRESHOLD
        temp_alert = pd.notna(row["max_temp"]) and row["max_temp"] > ANOMALY_TEMP_THRESHOLD
        return "ALERT" if (vib_alert or temp_alert) else "OK"

    agg["health_status"] = agg.apply(health_status, axis=1)
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
            already_alerting = conn.execute(
                "SELECT COUNT(*) FROM active_alerts WHERE machine_id = ? AND resolved = 0", (r["machine_id"],)
            ).fetchone()[0]
            if not already_alerting:
                reason = f"max_vibration={r['max_vibration']:.2f}, max_temp={r['max_temp']:.1f}"
                conn.execute(
                    "INSERT INTO active_alerts (machine_id, triggered_at, reason) VALUES (?, ?, ?)",
                    (r["machine_id"], str(r["window_end"]), reason),
                )
                alerts_written += 1
        else:
            conn.execute("UPDATE active_alerts SET resolved = 1 WHERE machine_id = ? AND resolved = 0", (r["machine_id"],))

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
    agg = compute_tumbling_windows(df, pd.Timestamp.now(tz="UTC"))
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

    if not args.watch:
        run_once()
        return

    print("Watching for new telemetry... (Ctrl+C to stop)")
    try:
        while True:
            run_once()
            time.sleep(args.poll)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
