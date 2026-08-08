"""
extract_mysql_to_s3.py
------------------------
Runs on Databricks. Extracts tables from MySQL via JDBC and lands them as
Parquet in the S3 raw zone, partitioned by extraction date.

Supports two modes:
  - full:        pulls the entire table (used for small dims, e.g. products)
  - incremental: pulls only rows where updated_at > last watermark
                  (used for customers, orders, inventory)

Intended to be run as a Databricks Job / Workflow task, one task per table,
or looped over TABLES_CONFIG in a single job for simplicity.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mysql_to_s3_extract").getOrCreate()

# ---------------------------------------------------------------------
# Config — in production these come from Databricks widgets / job params
# and secrets from AWS Secrets Manager / Databricks secret scopes.
# ---------------------------------------------------------------------
JDBC_URL = dbutils.secrets.get("ecommerce", "mysql_jdbc_url")
JDBC_USER = dbutils.secrets.get("ecommerce", "mysql_user")
JDBC_PASSWORD = dbutils.secrets.get("ecommerce", "mysql_password")

S3_RAW_BASE = "s3://ecommerce-data-lake/raw"
WATERMARK_TABLE_PATH = "s3://ecommerce-data-lake/_control/watermarks"

TABLES_CONFIG = {
    "customers":   {"mode": "incremental", "watermark_col": "updated_at"},
    "products":    {"mode": "full"},
    "orders":      {"mode": "incremental", "watermark_col": "updated_at"},
    "order_items": {"mode": "full"},       # small, immutable once created — full pull is simplest
    "inventory":   {"mode": "incremental", "watermark_col": "updated_at"},
}


def read_jdbc_table(table_name: str, predicate: str = None):
    reader = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name if not predicate else f"(SELECT * FROM {table_name} WHERE {predicate}) t")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", "10000")
    )
    return reader.load()


def get_last_watermark(table_name: str):
    """Reads the last successfully extracted watermark timestamp for a table."""
    path = f"{WATERMARK_TABLE_PATH}/{table_name}"
    try:
        df = spark.read.format("delta").load(path)
        row = df.orderBy(F.col("watermark_ts").desc()).first()
        return row["watermark_ts"] if row else "1970-01-01 00:00:00"
    except Exception:
        return "1970-01-01 00:00:00"


def save_watermark(table_name: str, new_watermark: str):
    path = f"{WATERMARK_TABLE_PATH}/{table_name}"
    df = spark.createDataFrame([(new_watermark, datetime.utcnow())], ["watermark_ts", "recorded_at"])
    df.write.format("delta").mode("append").save(path)


def extract_table(table_name: str, config: dict, run_date: str):
    if config["mode"] == "full":
        df = read_jdbc_table(table_name)
    else:
        watermark_col = config["watermark_col"]
        last_watermark = get_last_watermark(table_name)
        predicate = f"{watermark_col} > '{last_watermark}'"
        df = read_jdbc_table(table_name, predicate=predicate)

    df = df.withColumn("_extracted_at", F.current_timestamp()) \
           .withColumn("_source_table", F.lit(table_name))

    row_count = df.count()
    out_path = f"{S3_RAW_BASE}/{table_name}/dt={run_date}"
    df.write.mode("overwrite").parquet(out_path)

    if config["mode"] == "incremental" and row_count > 0:
        new_watermark = df.agg(F.max(config["watermark_col"])).first()[0]
        save_watermark(table_name, str(new_watermark))

    print(f"[{table_name}] mode={config['mode']} rows={row_count} -> {out_path}")
    return row_count


def main():
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    summary = {}
    for table_name, config in TABLES_CONFIG.items():
        try:
            summary[table_name] = extract_table(table_name, config, run_date)
        except Exception as e:
            print(f"[{table_name}] FAILED: {e}")
            raise
    print("Extraction summary:", summary)


if __name__ == "__main__":
    main()
