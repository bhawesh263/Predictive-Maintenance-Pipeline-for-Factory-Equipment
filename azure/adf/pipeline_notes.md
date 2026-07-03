# ADF Pipeline: pl_daily_reliability

1. **Get Metadata** activity - check new files landed in `raw/` since last run
2. **If Condition** - if no new files, skip run (log to a pipeline run log table)
3. **Validation** activity - lightweight schema check (e.g., a Databricks notebook or
   ADF Data Flow) that flags rows missing required fields, like the `bad_schema`
   events the simulator occasionally sends
4. **Databricks Notebook** activity - triggers `daily_reliability_job.py`
5. **Trigger:** scheduled nightly (e.g., 2 AM) via a Tumbling Window or Schedule trigger

See `local_sim/orchestration/orchestrator.py` for a runnable emulation of this
exact activity sequence.
