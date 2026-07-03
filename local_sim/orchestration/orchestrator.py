# Local stand-in for the ADF pipeline `pl_daily_reliability` (build guide, Step 6):
# check for new raw files, validate their schema (quarantining anything with a
# missing field - this is where the simulator's bad_schema events actually get
# caught, since the stream processor lets them through untouched), kick off the
# batch job, and log the run.
#
# python orchestrator.py                        # run once, now
# python orchestrator.py --schedule --hour 2     # loop forever, run once daily at 2am local time

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from batch import daily_reliability_job

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "storage" / "raw"
QUARANTINE_DIR = BASE_DIR / "storage" / "quarantine"
RUN_LOG_PATH = BASE_DIR / "logs" / "pipeline_runs.jsonl"
LAST_RUN_MARKER = BASE_DIR / "storage" / "checkpoints" / "orchestrator_last_run.json"

REQUIRED_FIELDS = ["machine_id", "event_time", "temperature", "vibration", "rpm", "status"]


def get_new_files_since(last_run_iso):
    """Stand-in for the ADF 'Get Metadata' activity."""
    if not RAW_DIR.exists():
        return []
    all_files = list(RAW_DIR.rglob("*.parquet"))
    if last_run_iso is None:
        return all_files
    last_run_ts = datetime.fromisoformat(last_run_iso).timestamp()
    return [f for f in all_files if f.stat().st_mtime > last_run_ts]


def validate_schema(files):
    """Quarantine any row missing a required field instead of letting it into the daily aggregates."""
    total_rows = 0
    quarantined_rows = 0
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    for f in files:
        df = pd.read_parquet(f)
        total_rows += len(df)

        if not set(REQUIRED_FIELDS).issubset(df.columns):
            continue

        bad_rows = df[df[REQUIRED_FIELDS].isna().any(axis=1)]
        if not bad_rows.empty:
            quarantined_rows += len(bad_rows)
            bad_rows.to_parquet(QUARANTINE_DIR / f"{f.stem}_quarantine.parquet", index=False)

    return total_rows, quarantined_rows


def load_last_run():
    if LAST_RUN_MARKER.exists():
        return json.loads(LAST_RUN_MARKER.read_text()).get("last_run")
    return None


def save_last_run(ts_iso):
    LAST_RUN_MARKER.parent.mkdir(parents=True, exist_ok=True)
    LAST_RUN_MARKER.write_text(json.dumps({"last_run": ts_iso}))


def log_run(record: dict):
    RUN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def run_pipeline_once():
    started_at = datetime.now(timezone.utc).isoformat()
    last_run = load_last_run()

    new_files = get_new_files_since(last_run)
    if not new_files:
        record = {"started_at": started_at, "status": "skipped_no_new_files", "files_seen": 0}
        log_run(record)
        print("No new files since last run - skipping.")
        return record

    total_rows, quarantined_rows = validate_schema(new_files)
    daily_reliability_job.main()
    save_last_run(started_at)

    record = {
        "started_at": started_at,
        "status": "success",
        "files_seen": len(new_files),
        "rows_validated": total_rows,
        "rows_quarantined": quarantined_rows,
    }
    log_run(record)
    print(f"Pipeline run complete: {len(new_files)} file(s), {total_rows} rows validated, "
          f"{quarantined_rows} row(s) quarantined for missing fields.")
    return record


def main():
    parser = argparse.ArgumentParser(description="Local emulator for the ADF pl_daily_reliability pipeline.")
    parser.add_argument("--schedule", action="store_true", help="loop forever, running once per day at --hour")
    parser.add_argument("--hour", type=int, default=2, help="local hour (0-23) to run at in --schedule mode")
    args = parser.parse_args()

    if not args.schedule:
        run_pipeline_once()
        return

    print(f"Scheduled mode: will run once daily at {args.hour:02d}:00 local time. Ctrl+C to stop.")
    last_run_date = None
    try:
        while True:
            now = datetime.now()
            if now.hour == args.hour and now.date() != last_run_date:
                run_pipeline_once()
                last_run_date = now.date()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
