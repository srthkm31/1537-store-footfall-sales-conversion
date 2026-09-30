# Databricks notebook source
SEED = 1537
MY_ID = "sarthakm"

CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw
""")

print("Volume path:", VOL)

# COMMAND ----------

import pyspark.sql.functions as F

# Seed
SEED = 1537

def W(d, n):
    d.write.mode('overwrite').option('header', True).csv(f'{VOL}/raw/{n}')

S = "if(hour between 18 and 20, 1.8, if(hour between 14 and 16, 0.6, 1.0))"

K = "if(dayofweek(trade_date) in (1, 7), 1.6, 1.0)"

C = "if(dayofweek(trade_date) in (1,7), .85, 1) * if(hour between 14 and 16, .45, 1)"

st = spark.range(12).selectExpr(
    "int(id) as s",
    "format_string('ST%02d', id + 1) as store_id",
    "format_string('CITY%02d', id % 6) as city",
    "if(id % 2 = 0, 'Mall', 'High Street') as format",
    f"round(0.08 + rand({SEED}) * 0.20, 3) as conv_base"
)

W(st.select('store_id', 'city', 'format'), 'stores')

gr = (
    st.crossJoin(
        spark.range(90).withColumnRenamed('id', 'd')
    )
    .crossJoin(
        spark.range(10, 22).withColumnRenamed('id', 'hour')
    )
    .selectExpr(
        "*",
        "date_add(date'2025-04-01', int(d)) as trade_date"
    )
)

gr = gr.selectExpr(
    "*",
    "(s * 90 + int(d)) * 12 + int(hour) - 10 as rn",
    f"int(round(30 * ({S}) * ({K}) * (0.7 + rand({SEED + 1}) * 0.6))) as fc"
)

gr = gr.selectExpr(
    "*",
    f"int(round(fc * conv_base * ({C}))) as nb"
)

OFF = "pmod(rn, 54) = 0"

ERR = "(pmod(rn, 54) = 1 or pmod(rn, 216) = 2)"

W(
    gr.selectExpr(
        "store_id",
        "trade_date",
        "hour",
        f"if({OFF}, 'OFFLINE', 'OK') as counter_status",
        f"case when {ERR} then 'ERR' when {OFF} then '0' else string(fc) end as footfall"
    ),
    'footfall'
)

bl = (
    gr.filter('nb > 0')
    .withColumn(
        'm',
        F.explode(F.sequence(F.lit(1), F.col('nb')))
    )
)

bl = bl.selectExpr(
    "format_string('B%02d%03d%02d%02d', s, int(d), int(hour), m) as bill_id",
    "store_id",
    "s * 90 + int(d) as k",
    "m",
    "hour",
    "format_string('%s %02d:%02d:00', string(trade_date), int(hour), m % 60) as bill_ts",
    f"int(1 + rand({SEED + 2}) * 8) as items",
    f"round(exp(6.0 + randn({SEED + 3}) * 0.7), 2) as bill_amount"
)

B = ['bill_id', 'store_id', 'bill_ts', 'items', 'bill_amount']

dup = bl.filter(
    "hour = 12 and m = 1 and pmod(k, 8) = 3 and k < 1040"
)

ex = spark.range(135).selectExpr(
    "format_string('B9%06d', id) as bill_id",
    "if(id < 75, 'ST13', format_string('ST%02d', id % 12 + 1)) as store_id",
    "format_string('2025-05-%02d %02d:15:00', int(id) % 28 + 1, if(id < 75, 11 + int(id) % 9, 3 + int(id) % 3)) as bill_ts",
    "int(1 + pmod(id, 8)) as items",
    "round(400 + pmod(id, 900), 2) as bill_amount"
)

W(
    bl.select(*B)
      .unionByName(dup.select(*B))
      .unionByName(ex.select(*B)),
    'bills'
)

# COMMAND ----------

print("Stores:")
display(spark.read.option("header", True).csv(f"{VOL}/raw/stores").count())

print("Footfall:")
display(spark.read.option("header", True).csv(f"{VOL}/raw/footfall").count())

print("Bills:")
display(spark.read.option("header", True).csv(f"{VOL}/raw/bills").count())