"""
app.py

Local stand-in for the Power BI dashboard described in the build guide (Step 7).
Reads from the same two sinks a real Power BI report would connect to:

  - SQLite hot path (machine_status, active_alerts) -> live/hot view
  - Gold Delta/parquet table (curated/gold_reliability.parquet) -> daily trends

Panels:
  1. Machine status grid - one tile per machine, color-coded by latest health_status
  2. Alert feed - active_alerts, most recent first
  3. Daily downtime-risk trend - uptime_score per machine over time

Run:
    streamlit run app.py
"""
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent.parent / "local_sim"
HOT_PATH_DB = BASE_DIR / "storage" / "hot_path" / "hot_path.db"
GOLD_PATH = BASE_DIR / "storage" / "curated" / "gold_reliability.parquet"

st.set_page_config(page_title="Factory Reliability Dashboard", layout="wide")
st.title("🏭 Predictive Maintenance Dashboard")
st.caption("Local emulation of the Power BI report - same sinks (hot-path SQL + curated Delta table), different renderer.")

if st.button("🔄 Refresh"):
    st.rerun()

if not HOT_PATH_DB.exists():
    st.warning("No hot-path data yet. Run the simulator + stream processor first (see README).")
    st.stop()

conn = sqlite3.connect(HOT_PATH_DB)
status_df = pd.read_sql_query("SELECT * FROM machine_status ORDER BY window_end DESC", conn)
alerts_df = pd.read_sql_query("SELECT * FROM active_alerts ORDER BY triggered_at DESC", conn)
conn.close()

st.header("Machine Status Grid")
if status_df.empty:
    st.info("No windowed status yet - the stream processor hasn't closed a 5-minute window.")
else:
    latest = status_df.sort_values("window_end").groupby("machine_id").tail(1)
    cols = st.columns(min(len(latest), 6) or 1)
    for i, (_, row) in enumerate(latest.sort_values("machine_id").iterrows()):
        color = "🔴" if row["health_status"] == "ALERT" else "🟢"
        with cols[i % len(cols)]:
            st.metric(
                label=f"{color} {row['machine_id']}",
                value=row["health_status"],
                delta=f"{row['avg_temp']:.1f}°C / vib {row['avg_vibration']:.2f}" if pd.notna(row["avg_vibration"]) else f"{row['avg_temp']:.1f}°C",
            )

st.header("Alert Feed")
if alerts_df.empty:
    st.info("No alerts yet.")
else:
    unresolved = alerts_df[alerts_df["resolved"] == 0]
    st.dataframe(alerts_df, use_container_width=True, hide_index=True)
    st.caption(f"{len(unresolved)} unresolved alert(s)")

st.header("Daily Downtime-Risk Trend")
if not GOLD_PATH.exists():
    st.info("No gold table yet - run the orchestrator/batch job to compute daily reliability metrics.")
else:
    gold_df = pd.read_parquet(GOLD_PATH)
    if gold_df.empty:
        st.info("Gold table is empty.")
    else:
        pivot = gold_df.pivot_table(index="date", columns="machine_id", values="uptime_score")
        st.line_chart(pivot)
        st.dataframe(gold_df.sort_values(["date", "machine_id"]), use_container_width=True, hide_index=True)
