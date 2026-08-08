"""
bronze.py
---------
Bronze layer: loads raw Parquet extracts from S3 into Delta tables with
minimal transformation (schema enforcement + metadata only). No business
logic, no dedup, no cleaning — bronze is the "as received from source"
layer so we can always replay downstream transformations if silver/gold
logic changes.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("bronze_layer").getOrCreate()

S3_RAW_BASE = "s3://ecommerce-data-lake/raw"
BRONZE_DB = "ecommerce_bronze"

TABLES = ["customers", "products", "orders", "order_items", "inventory"]


def load_raw_to_bronze(table_name: str, run_date: str) -> DataFrame:
    raw_path = f"{S3_RAW_BASE}/{table_name}/dt={run_date}"
    df = spark.read.parquet(raw_path)

    df = df.withColumn("_bronze_loaded_at", F.current_timestamp()) \
           .withColumn("_source_file", F.input_file_name())

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    target_table = f"{BRONZE_DB}.{table_name}"

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
    return df


def run(run_date: str):
    for table in TABLES:
        df = load_raw_to_bronze(table, run_date)
        print(f"[bronze] {table}: loaded {df.count()} rows")


if __name__ == "__main__":
    import sys
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(run_date)
