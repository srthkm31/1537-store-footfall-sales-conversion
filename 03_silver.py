# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


MY_ID = "sarthakm"

CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

BRONZE = f"{VOL}/bronze"
SILVER = f"{VOL}/silver"

print("Bronze:", BRONZE)
print("Silver:", SILVER)

# COMMAND ----------


footfall_bronze = (
    spark.read
    .format("delta")
    .load(f"{BRONZE}/footfall")
)



footfall_typed = (
    footfall_bronze
    .withColumn(
        "footfall_cast",
        F.expr("try_cast(footfall AS INT)")
    )
    .withColumn(
        "trade_date",
        F.expr("try_cast(trade_date AS DATE)")
    )
    .withColumn(
        "hour",
        F.expr("try_cast(hour AS INT)")
    )
)

err_count = (
    footfall_typed
    .filter(
        (F.col("footfall") == "ERR") &
        F.col("footfall_cast").isNull()
    )
    .count()
)

print("ERR values converted to NULL:", err_count)



offline_count = (
    footfall_typed
    .filter(F.col("counter_status") == "OFFLINE")
    .count()
)

print("OFFLINE rows:", offline_count)



footfall_silver = (
    footfall_typed
    .withColumn(
        "footfall",
        F.when(
            F.col("counter_status") == "OFFLINE",
            F.lit(None).cast("INT")
        )
        .otherwise(F.col("footfall_cast"))
    )
    .withColumn(
        "sensor_ok",
        (
            F.col("counter_status") != "OFFLINE"
        )
        &
        F.col("footfall").isNotNull()
        &
        (F.col("footfall") > 0)
    )
    .drop("footfall_cast")
)


dq_footfall = (
    footfall_typed
    .filter(
        (F.col("footfall") == "ERR") &
        F.col("footfall_cast").isNull()
    )
    .select(
        "store_id",
        "trade_date",
        "hour",
        "counter_status",
        "footfall",
        "_source_file",
        "_ingested_at",
        "_row_hash"
    )
    .withColumn(
        "reason",
        F.lit("unparseable_footfall")
    )
)

dq_footfall.write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"{SILVER}/data_quality_footfall")


footfall_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"{SILVER}/footfall")

print("Silver footfall created successfully.")

# COMMAND ----------


bills_bronze = (
    spark.read
    .format("delta")
    .load(f"{BRONZE}/bills")
)


stores_bronze = (
    spark.read
    .format("delta")
    .load(f"{BRONZE}/stores")
    .select(
        "store_id",
        "city",
        "format"
    )
)



bills_before = bills_bronze.count()

print("Bills before deduplication:", bills_before)



bills_typed = (
    bills_bronze
    .withColumn(
        "bill_ts",
        F.expr("try_cast(bill_ts AS TIMESTAMP)")
    )
    .withColumn(
        "items",
        F.expr("try_cast(items AS INT)")
    )
    .withColumn(
        "bill_amount",
        F.expr("try_cast(bill_amount AS DOUBLE)")
    )
)



dedupe_window = (
    Window
    .partitionBy("bill_id")
    .orderBy(F.col("_ingested_at").desc())
)

bills_deduped = (
    bills_typed
    .withColumn(
        "_rn",
        F.row_number().over(dedupe_window)
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

bills_after_dedupe = bills_deduped.count()

duplicates_removed = (
    bills_before - bills_after_dedupe
)

print("Bills after deduplication:", bills_after_dedupe)
print("Duplicate bills removed:", duplicates_removed)



bills_with_hour = (
    bills_deduped
    .withColumn(
        "bill_hour",
        F.hour("bill_ts")
    )
)



bills_checked = (
    bills_with_hour
    .join(
        stores_bronze,
        on="store_id",
        how="left"
    )
    .withColumn(
        "reject_reason",
        F.when(
            F.col("city").isNull(),
            F.lit("unknown_store")
        )
        .when(
            (F.col("bill_hour") < 10) |
            (F.col("bill_hour") > 21),
            F.lit("outside_trading_hours")
        )
    )
)



bills_rejected = (
    bills_checked
    .filter(F.col("reject_reason").isNotNull())
)

print("Reject counts:")
bills_rejected.groupBy("reject_reason").count().show()

bills_rejected.write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"{SILVER}/bill_rejects")



bills_silver = (
    bills_checked
    .filter(F.col("reject_reason").isNull())
    .select(
        "bill_id",
        "store_id",
        "bill_ts",
        "items",
        "bill_amount",
        "_source_file",
        "_ingested_at",
        "_row_hash"
    )
)



bills_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"{SILVER}/bills")

print("Silver bills created successfully.")

# COMMAND ----------

print("========== SILVER VALIDATION ==========")

print()
print("FOOTFALL")
print("---------------------------------------")
print("Bronze footfall:",
      footfall_bronze.count())

print("ERR -> NULL:",
      err_count)

print("OFFLINE rows:",
      offline_count)

print("Silver footfall:",
      footfall_silver.count())

print()
print("BILLS")
print("---------------------------------------")
print("Bronze bills:",
      bills_before)

print("After deduplication:",
      bills_after_dedupe)

print("Duplicates removed:",
      duplicates_removed)

print("Silver bills:",
      bills_silver.count())

print()
print("REJECTIONS")
print("---------------------------------------")

reject_counts = (
    bills_rejected
    .groupBy("reject_reason")
    .count()
    .orderBy("reject_reason")
)

reject_counts.show()

print("=======================================")