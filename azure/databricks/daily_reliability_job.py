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
