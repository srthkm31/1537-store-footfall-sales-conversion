# Databricks notebook source
from pyspark.sql import functions as F

VOL = "/Volumes/workspace/capstone_sarthakm/raw"
SILVER = f"{VOL}/silver"
BRONZE = f"{VOL}/bronze"
GOLD = f"{VOL}/gold"

footfall_silver = spark.read.format("delta").load(f"{SILVER}/footfall")
bills_silver = spark.read.format("delta").load(f"{SILVER}/bills")

stores = (
    spark.read.format("delta")
    .load(f"{BRONZE}/stores")
    .select("store_id", "city", "format")
)

print("Footfall Silver:", footfall_silver.count())
print("Bills Silver:", bills_silver.count())
print("Stores:", stores.count())

print("\nFootfall schema:")
footfall_silver.printSchema()

print("\nBills schema:")
bills_silver.printSchema()

# COMMAND ----------

f = (
    footfall_silver
    .select(
        "store_id",
        F.to_date("trade_date").alias("trade_date"),
        F.col("hour").cast("int").alias("hour"),
        F.col("footfall").cast("int").alias("footfall")
    )
)

f = f.withColumn(
    "sensor_ok",
    (F.col("footfall").isNotNull()) & (F.col("footfall") > 0)
)

b = (
    bills_silver
    .select(
        "bill_id",
        "store_id",
        F.to_timestamp("bill_ts").alias("bill_ts"),
        F.col("bill_amount").cast("double").alias("bill_amount")
    )
    .withColumn("trade_date", F.to_date("bill_ts"))
    .withColumn("hour", F.hour("bill_ts"))
)

bill_hourly = (
    b.groupBy("store_id", "trade_date", "hour")
    .agg(
        F.count("bill_id").alias("bills"),
        F.round(F.sum("bill_amount"), 2).alias("revenue")
    )
)

gold = (
    f
    .join(
        bill_hourly,
        on=["store_id", "trade_date", "hour"],
        how="left"
    )
    .join(
        stores,
        on="store_id",
        how="left"
    )
    .withColumn(
        "bills",
        F.coalesce(F.col("bills"), F.lit(0)).cast("long")
    )
    .withColumn(
        "revenue",
        F.coalesce(F.col("revenue"), F.lit(0.0))
    )
    .withColumn(
        "is_weekend",
        F.dayofweek("trade_date").isin([1, 7])
    )
    .withColumn(
        "conversion_rate",
        F.when(
            F.col("sensor_ok"),
            F.try_divide(
                F.col("bills").cast("double"),
                F.col("footfall").cast("double")
            )
        )
    )
    .select(
        "store_id",
        "city",
        "format",
        "trade_date",
        "hour",
        "is_weekend",
        "footfall",
        "bills",
        "revenue",
        "conversion_rate",
        "sensor_ok"
    )
)

print("Gold rows:", gold.count())

# COMMAND ----------

gold.write.mode("overwrite").format("delta").save(f"{GOLD}/store_hour")

gold_check = spark.read.format("delta").load(f"{GOLD}/store_hour")

total_rows = gold_check.count()

sensor_bad = gold_check.filter(~F.col("sensor_ok")).count()
sensor_good = gold_check.filter(F.col("sensor_ok")).count()

gold_bill_count = gold_check.agg(
    F.sum("bills").alias("total_bills")
).first()["total_bills"]

silver_bill_count = bills_silver.count()

gold_revenue = gold_check.agg(
    F.sum("revenue").alias("total_revenue")
).first()["total_revenue"]

silver_revenue = bills_silver.agg(
    F.sum(F.col("bill_amount").cast("double")).alias("total_revenue")
).first()["total_revenue"]

print("========== GOLD VALIDATION ==========")
print("Gold rows:", total_rows)
print("sensor_ok = TRUE:", sensor_good)
print("sensor_ok = FALSE:", sensor_bad)

print("\n========== BILL VALIDATION ==========")
print("Silver bills:", silver_bill_count)
print("Gold bills:", gold_bill_count)

print("\n========== REVENUE VALIDATION ==========")
print("Silver revenue:", silver_revenue)
print("Gold revenue:", gold_revenue)

duplicate_keys = (
    gold_check
    .groupBy("store_id", "trade_date", "hour")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\nDuplicate store-hour keys:", duplicate_keys)

required_columns = [
    "store_id",
    "city",
    "format",
    "trade_date",
    "hour",
    "is_weekend",
    "footfall",
    "bills",
    "revenue",
    "conversion_rate",
    "sensor_ok"
]

assert total_rows == 12960, \
    f"Expected 12960 rows, got {total_rows}"

assert sensor_bad == 540, \
    f"Expected 540 bad sensor rows, got {sensor_bad}"

assert sensor_good == 12420, \
    f"Expected 12420 good rows, got {sensor_good}"

assert duplicate_keys == 0, \
    "Gold contains duplicate store/date/hour combinations"

assert gold_bill_count == silver_bill_count, \
    f"Gold bills {gold_bill_count} != Silver bills {silver_bill_count}"

assert abs(gold_revenue - silver_revenue) <= 1.0, \
    f"Revenue mismatch: Gold={gold_revenue}, Silver={silver_revenue}"

assert gold_check.columns == required_columns, \
    f"Unexpected columns/order: {gold_check.columns}"

print("\nALL GOLD CHECKS PASSED.")

# COMMAND ----------

display(
    gold_check
    .orderBy("store_id", "trade_date", "hour")
    .limit(30)
)