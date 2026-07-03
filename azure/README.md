# Azure deployment reference

These are the real Azure artifacts from the original build guide
(`predictive-maintenance-pipeline-guide.md` at the repo root), extracted as
standalone files so you can deploy them once you have an Azure subscription:

| File | Azure service |
|---|---|
| `simulator/sensor_simulator_azure.py` | IoT Hub device SDK simulator |
| `stream_analytics/query.saql` | Stream Analytics job query |
| `sql/schema.sql` | Azure SQL hot-path tables |
| `databricks/daily_reliability_job.py` | Databricks PySpark + Delta batch job |
| `adf/pipeline_notes.md` | Data Factory pipeline activity sequence |

Follow the numbered `az` CLI commands and step-by-step instructions in the main
guide for provisioning. Everything in `local_sim/` at the repo root is a
runnable, no-Azure-account-required emulation of this same architecture -
useful for developing/testing the logic before you pay for real resources.
