# Databricks notebook source
MY_ID = "sarthakm"

CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

RAW_FILES = f"{VOL}/raw"
BRONZE = f"{VOL}/bronze"

print("Input files:", RAW_FILES)
print("Bronze path:", BRONZE)

# COMMAND ----------

from pyspark.sql import functions as F


def create_bronze(name):


    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{RAW_FILES}/{name}")
    )


    raw_columns = df.columns


    df = (
        df
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(c), F.lit(""))
                        for c in raw_columns
                    ]
                ),
                256
            )
        )
    )


    (
        df.write
        .mode("overwrite")
        .format("delta")
        .save(f"{BRONZE}/{name}")
    )

    return df


stores_bronze = create_bronze("stores")
footfall_bronze = create_bronze("footfall")
bills_bronze = create_bronze("bills")

print("Bronze layer created successfully.")

# COMMAND ----------

print("Stores Bronze:", spark.read.format("delta").load(f"{BRONZE}/stores").count())
print("Footfall Bronze:", spark.read.format("delta").load(f"{BRONZE}/footfall").count())
print("Bills Bronze:", spark.read.format("delta").load(f"{BRONZE}/bills").count())