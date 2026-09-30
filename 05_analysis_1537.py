# Databricks notebook source
# MAGIC %md
# MAGIC # 05_analysis — Store Footfall vs Sales Conversion
# MAGIC
# MAGIC **Roll No.: 1537**  
# MAGIC **Generator seed: 1537**
# MAGIC
# MAGIC This notebook reads the Gold `store_hour` dataset and answers three business questions:
# MAGIC
# MAGIC 1. Which stores have the highest average conversion rate?
# MAGIC 2. How does conversion differ between weekdays and weekends?
# MAGIC 3. Which store-hour combinations generate the highest revenue?
# MAGIC
# MAGIC It also exports the Gold dataset to CSV for the Snowflake loading step.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# Keep this path consistent with the 04_gold notebook that successfully ran.
VOL = "/Volumes/workspace/capstone_sarthakm/raw"
GOLD = f"{VOL}/gold"
EXPORT = f"{VOL}/export/gold_csv"

gold = spark.read.format("delta").load(f"{GOLD}/store_hour")

print("Gold rows:", gold.count())
display(gold.limit(10))


# COMMAND ----------

# Validation before analysis
total_rows = gold.count()
sensor_ok = gold.filter(F.col("sensor_ok") == True).count()
sensor_bad = gold.filter(F.col("sensor_ok") == False).count()

assert total_rows == 12960, f"Expected 12960 Gold rows, found {total_rows}"

print("Gold validation")
print("Total rows:", total_rows)
print("Usable sensor rows:", sensor_ok)
print("Unusable sensor rows:", sensor_bad)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — Which stores have the highest average conversion rate?
# MAGIC
# MAGIC Only store-hours with `sensor_ok = TRUE` are included. The result calculates the average of the hourly conversion rates for each store.
# MAGIC

# COMMAND ----------

q1 = (
    gold
    .filter(F.col("sensor_ok") == True)
    .groupBy("store_id", "city", "format")
    .agg(
        F.round(F.avg("conversion_rate"), 4).alias("avg_conversion_rate"),
        F.count("*").alias("usable_store_hours"),
        F.sum("bills").alias("total_bills"),
        F.sum("footfall").alias("total_footfall")
    )
    .orderBy(F.col("avg_conversion_rate").desc())
)

display(q1)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — How does conversion differ between weekdays and weekends?
# MAGIC
# MAGIC The comparison aggregates usable store-hours into weekday and weekend groups.
# MAGIC

# COMMAND ----------

q2 = (
    gold
    .filter(F.col("sensor_ok") == True)
    .groupBy(
        F.when(F.col("is_weekend") == True, "Weekend")
         .otherwise("Weekday")
         .alias("day_type")
    )
    .agg(
        F.sum("footfall").alias("total_footfall"),
        F.sum("bills").alias("total_bills"),
        F.round(
            F.sum("bills") / F.nullif(F.sum("footfall"), F.lit(0)),
            4
        ).alias("conversion_rate"),
        F.round(F.sum("revenue"), 2).alias("total_revenue")
    )
    .orderBy("day_type")
)

display(q2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 — Which store-hour combinations generate the highest revenue?
# MAGIC
# MAGIC This ranks individual store/date/hour records by revenue while retaining the conversion and footfall context.
# MAGIC

# COMMAND ----------

q3 = (
    gold
    .filter(F.col("sensor_ok") == True)
    .select(
        "store_id", "city", "format", "trade_date", "hour",
        "footfall", "bills", "revenue", "conversion_rate"
    )
    .orderBy(F.col("revenue").desc())
)

display(q3.limit(20))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Gold CSV for Snowflake
# MAGIC
# MAGIC The export contains the complete Gold table and is used for the Snowflake `GOLD_STAGE` → `GOLD_STORE_HOUR` load.
# MAGIC

# COMMAND ----------

# Write a single CSV output directory.
# Snowflake can load the CSV file produced inside this directory.
(
    gold
    .orderBy("store_id", "trade_date", "hour")
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(EXPORT)
)

print("Gold CSV export directory:", EXPORT)
display(dbutils.fs.ls(EXPORT))


# COMMAND ----------

# Final analysis summary for screenshots/report
print("========================================")
print("05_analysis COMPLETE")
print("========================================")
print("Gold rows:", total_rows)
print("Usable sensor rows:", sensor_ok)
print("Unusable sensor rows:", sensor_bad)

print("\nQ1 — Top stores by average conversion")
display(q1.limit(5))

print("\nQ2 — Weekday vs Weekend")
display(q2)

print("\nQ3 — Top 10 revenue store-hours")
display(q3.limit(10))
