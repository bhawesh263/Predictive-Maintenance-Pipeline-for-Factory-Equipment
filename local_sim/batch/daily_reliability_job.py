# Pandas version of azure/databricks/daily_reliability_job.py - same steps, no
# Spark cluster needed: read raw parquet, dedup, roll up daily reliability
# metrics per machine, upsert into a "gold" parquet table.
#
# python daily_reliability_job.py

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "storage" / "raw"
GOLD_PATH = BASE_DIR / "storage" / "curated" / "gold_reliability.parquet"

ANOMALY_TEMP_THRESHOLD = 90
ANOMALY_VIBRATION_THRESHOLD = 1.8


def read_raw() -> pd.DataFrame:
    files = list(RAW_DIR.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    return df


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest reading per machine per minute - handles late/duplicate deliveries."""
    if df.empty:
        return df

    df = df.dropna(subset=["event_time", "machine_id"]).copy()
    df["minute_bucket"] = df["event_time"].dt.floor("min")
    df = df.sort_values("event_time", ascending=False)
    df = df.drop_duplicates(subset=["machine_id", "minute_bucket"], keep="first")
    return df.drop(columns=["minute_bucket"])


def compute_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["machine_id", "date", "total_readings", "anomaly_count", "avg_daily_temp", "avg_daily_vibration", "uptime_score"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    df = df.copy()
    df["date"] = df["event_time"].dt.date.astype(str)
    df["is_anomaly"] = (df["temperature"] > ANOMALY_TEMP_THRESHOLD) | (df["vibration"] > ANOMALY_VIBRATION_THRESHOLD)

    daily = (
        df.groupby(["machine_id", "date"])
        .agg(
            total_readings=("machine_id", "size"),
            anomaly_count=("is_anomaly", "sum"),
            avg_daily_temp=("temperature", "mean"),
            avg_daily_vibration=("vibration", "mean"),
        )
        .reset_index()
    )
    daily["uptime_score"] = 1 - (daily["anomaly_count"] / daily["total_readings"])
    return daily


def merge_into_gold(daily_metrics: pd.DataFrame):
    """Upsert: any (machine_id, date) in this batch replaces what's already in the gold table."""
    GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)

    if daily_metrics.empty:
        print("No metrics to merge.")
        return

    if GOLD_PATH.exists():
        existing = pd.read_parquet(GOLD_PATH)
        stale = pd.MultiIndex.from_frame(existing[["machine_id", "date"]])
        incoming = pd.MultiIndex.from_frame(daily_metrics[["machine_id", "date"]])
        existing = existing[~stale.isin(incoming)]
        merged = pd.concat([existing, daily_metrics], ignore_index=True)
    else:
        merged = daily_metrics

    merged = merged.sort_values(["date", "machine_id"]).reset_index(drop=True)
    merged.to_parquet(GOLD_PATH, index=False)


def main():
    raw_df = read_raw()
    if raw_df.empty:
        print("No raw data found - run the simulator and stream processor first.")
        return

    dedup_df = dedup(raw_df)
    daily_metrics = compute_daily_metrics(dedup_df)
    merge_into_gold(daily_metrics)

    print(f"Read {len(raw_df)} raw rows, {len(dedup_df)} after dedup.")
    print(f"Wrote {len(daily_metrics)} daily metric row(s) to {GOLD_PATH}")
    print(daily_metrics.to_string(index=False))


if __name__ == "__main__":
    main()
