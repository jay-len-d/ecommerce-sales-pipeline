"""
silver.py
---------
Silver layer: cleans and conforms bronze data.

Design note: transformation logic is written as small, pure functions
(DataFrame in -> DataFrame out) with no I/O inside them. This is what
makes them unit-testable without spinning up Databricks — see
tests/test_silver_transformations.py.

I/O (reading bronze tables, writing silver tables) lives only in run().
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_DB = "ecommerce_bronze"
SILVER_DB = "ecommerce_silver"


# ---------------------------------------------------------------------
# Pure transformation functions (unit tested)
# ---------------------------------------------------------------------

def clean_customers(df: DataFrame) -> DataFrame:
    """
    - Deduplicates on customer_id, keeping the most recently updated record
    - Standardizes name casing
    - Trims whitespace on string fields
    - Flags records with missing/invalid email as low quality (kept, not dropped)
    - Standardizes loyalty_tier to uppercase and defaults invalid values to BRONZE
    """
    valid_tiers = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]

    df = df.withColumn("first_name", F.initcap(F.trim(F.col("first_name")))) \
           .withColumn("last_name", F.initcap(F.trim(F.col("last_name")))) \
           .withColumn("email", F.lower(F.trim(F.col("email")))) \
           .withColumn(
               "loyalty_tier",
               F.when(F.upper(F.trim(F.col("loyalty_tier"))).isin(valid_tiers),
                      F.upper(F.trim(F.col("loyalty_tier"))))
                .otherwise(F.lit("BRONZE"))
           ) \
           .withColumn(
               "is_email_valid",
               F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
           )

    # Deduplicate: keep latest row per customer_id by updated_at
    window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    df = df.withColumn("_rn", F.row_number().over(window)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    return df


def clean_orders(df: DataFrame) -> DataFrame:
    """
    - Deduplicates on order_id
    - Standardizes order_status to uppercase
    - Drops orders with null customer_id or non-positive shipping_cost data errors
      into a "rejected" bucket (returned separately, not silently dropped)
    Returns a tuple-like struct via two DataFrames is avoided here for simplicity;
    instead we add a `_dq_valid` flag so callers can filter/quarantine explicitly.
    """
    df = df.withColumn("order_status", F.upper(F.trim(F.col("order_status")))) \
           .withColumn(
               "_dq_valid",
               (F.col("customer_id").isNotNull()) & (F.col("shipping_cost") >= 0)
           )

    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    df = df.withColumn("_rn", F.row_number().over(window)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

    return df


def calculate_line_total(df: DataFrame) -> DataFrame:
    """Adds a computed net line total accounting for discount_pct."""
    return df.withColumn(
        "line_total",
        F.round(
            F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100.0),
            2
        )
    )


def quarantine_invalid(df: DataFrame, flag_col: str = "_dq_valid"):
    """Splits a DataFrame into (valid, quarantined) based on a boolean flag column."""
    valid = df.filter(F.col(flag_col) == True).drop(flag_col)          # noqa: E712
    quarantined = df.filter(F.col(flag_col) == False).drop(flag_col)   # noqa: E712
    return valid, quarantined


# ---------------------------------------------------------------------
# I/O orchestration (not unit tested directly; relies on the functions above)
# ---------------------------------------------------------------------

def run():
    spark = SparkSession.builder.appName("silver_layer").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

    customers_bronze = spark.table(f"{BRONZE_DB}.customers")
    customers_silver = clean_customers(customers_bronze)
    customers_silver.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{SILVER_DB}.customers")

    orders_bronze = spark.table(f"{BRONZE_DB}.orders")
    orders_cleaned = clean_orders(orders_bronze)
    orders_valid, orders_quarantined = quarantine_invalid(orders_cleaned)
    orders_valid.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.orders")
    orders_quarantined.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{SILVER_DB}.orders_quarantine")

    order_items_bronze = spark.table(f"{BRONZE_DB}.order_items")
    order_items_silver = calculate_line_total(order_items_bronze)
    order_items_silver.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{SILVER_DB}.order_items")

    products_bronze = spark.table(f"{BRONZE_DB}.products")
    products_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.products")

    inventory_bronze = spark.table(f"{BRONZE_DB}.inventory")
    inventory_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.inventory")


if __name__ == "__main__":
    run()
