"""
gold_scd2.py
------------
Gold layer: builds dim_customer as a Slowly Changing Dimension Type 2,
tracking history of changes to loyalty_tier, address, city, state,
postal_code, and email over time.

Core logic (compute_scd2_snapshot, add_row_hash) is written as pure
functions operating on DataFrames so it can be unit tested in isolation
-- see tests/test_scd2.py. I/O (reading silver, writing the Delta table)
lives only in run_dim_customer_scd2().

SCD2 semantics implemented here:
  - Each business key (customer_id) can have multiple rows, each
    representing one "version" of that customer.
  - is_current = True marks the currently active version.
  - effective_start_date / effective_end_date define the validity window.
  - A change in any tracked attribute closes out (expires) the old row
    and opens a new row with a new surrogate key.
  - Untracked attribute changes (e.g. a typo fix that isn't in
    TRACKED_COLUMNS) do NOT trigger a new version -- this is a deliberate
    modeling choice, not an oversight.
  - Customers missing from the latest source snapshot are left as-is
    (no automatic expiration on deletion). If hard-delete tracking is
    needed later, add an explicit `is_deleted` flag from source instead.
"""

from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DateType

SILVER_DB = "ecommerce_silver"
GOLD_DB = "ecommerce_gold"

BUSINESS_KEY = "customer_id"
TRACKED_COLUMNS = ["loyalty_tier", "address", "city", "state", "postal_code", "email"]
PASSTHROUGH_COLUMNS = ["first_name", "last_name", "country"]  # carried along but not version-triggering
FAR_FUTURE_DATE = date(9999, 12, 31)

DIM_CUSTOMER_SCHEMA_EXTRA = [
    "customer_sk", "row_hash", "effective_start_date", "effective_end_date", "is_current"
]


# ---------------------------------------------------------------------
# Pure functions (unit tested)
# ---------------------------------------------------------------------

def add_row_hash(df: DataFrame, tracked_cols: list) -> DataFrame:
    """Adds a deterministic hash over tracked columns to detect changes cheaply."""
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in tracked_cols])
    return df.withColumn("row_hash", F.sha2(concat_expr, 256))


def _empty_dim_schema(source_df: DataFrame) -> StructType:
    fields = [f for f in source_df.schema.fields]
    fields += [
        StructField("customer_sk", LongType(), False),
        StructField("row_hash", StringType(), False),
        StructField("effective_start_date", DateType(), False),
        StructField("effective_end_date", DateType(), False),
        StructField("is_current", BooleanType(), False),
    ]
    return StructType(fields)


def initialize_dimension(source_df: DataFrame, tracked_cols: list, as_of_date: date) -> DataFrame:
    """Bootstraps dim_customer from a source snapshot when no dimension exists yet."""
    hashed = add_row_hash(source_df, tracked_cols)
    windowed = hashed.withColumn(
        "customer_sk", F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy(BUSINESS_KEY)
        )
    )
    return (
        windowed
        .withColumn("effective_start_date", F.lit(as_of_date))
        .withColumn("effective_end_date", F.lit(FAR_FUTURE_DATE))
        .withColumn("is_current", F.lit(True))
    )


def compute_scd2_snapshot(
    existing_dim: DataFrame,
    source_df: DataFrame,
    as_of_date: date,
    business_key: str = BUSINESS_KEY,
    tracked_cols: list = None,
) -> DataFrame:
    """
    Given the current state of dim_customer (existing_dim) and a fresh
    snapshot from silver.customers (source_df), returns the FULL new
    dimension table reflecting:
      - unchanged current rows carried forward as-is
      - changed rows: old version expired, new version inserted
      - brand-new business keys: inserted as new current rows
      - historical (already-expired) rows carried forward untouched

    This recomputes the whole table rather than doing an incremental
    Delta MERGE, which keeps the logic simple and fully unit-testable.
    For very large dimensions, the same logic maps directly onto a
    Delta `MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED` statement.
    """
    if tracked_cols is None:
        tracked_cols = TRACKED_COLUMNS

    spark = source_df.sparkSession

    if existing_dim is None or existing_dim.rdd.isEmpty():
        return initialize_dimension(source_df, tracked_cols, as_of_date)

    # Break Catalyst lineage before joining. Without this, calling this function
    # repeatedly on its own prior output (as happens when applying multiple
    # successive SCD2 snapshots in the same Spark session -- e.g. in tests, or
    # in a notebook that loops over historical batches) can produce ambiguous
    # attribute-resolution errors during the self-join, because the plan still
    # carries internal column IDs from earlier joins in existing_dim's lineage.
    # Reading fresh from a Delta table each run (the production path) avoids
    # this naturally; we materialize here so the function is safe either way.
    existing_dim = spark.createDataFrame(existing_dim.rdd, existing_dim.schema)
    source_df = spark.createDataFrame(source_df.rdd, source_df.schema)

    source_hashed = add_row_hash(source_df, tracked_cols).withColumnRenamed("row_hash", "new_row_hash")

    current_rows = existing_dim.filter(F.col("is_current") == True)          # noqa: E712
    historical_rows = existing_dim.filter(F.col("is_current") == False)      # noqa: E712

    # Compare current dim rows against the incoming source snapshot
    joined = current_rows.alias("dim").join(
        source_hashed.alias("src"), on=business_key, how="full_outer"
    )

    unchanged = joined.filter(
        F.col("dim.row_hash").isNotNull()
        & F.col("src.new_row_hash").isNotNull()
        & (F.col("dim.row_hash") == F.col("src.new_row_hash"))
    ).select("dim.*")

    changed_old = joined.filter(
        F.col("dim.row_hash").isNotNull()
        & F.col("src.new_row_hash").isNotNull()
        & (F.col("dim.row_hash") != F.col("src.new_row_hash"))
    ).select("dim.*") \
     .withColumn("effective_end_date", F.lit(as_of_date - timedelta(days=1))) \
     .withColumn("is_current", F.lit(False))

    changed_new_src = joined.filter(
        F.col("dim.row_hash").isNotNull()
        & F.col("src.new_row_hash").isNotNull()
        & (F.col("dim.row_hash") != F.col("src.new_row_hash"))
    ).select("src.*")

    brand_new_src = joined.filter(F.col("dim.row_hash").isNull() & F.col("src.new_row_hash").isNotNull()) \
        .select("src.*")

    # Rows present in the dim but absent from source: no source data to update,
    # so they're carried forward unchanged (see docstring on deletion semantics).
    missing_from_source = joined.filter(F.col("src.new_row_hash").isNull() & F.col("dim.row_hash").isNotNull()) \
        .select("dim.*")

    new_versions_needed = changed_new_src.unionByName(brand_new_src) \
        .withColumnRenamed("new_row_hash", "row_hash")

    max_sk_row = existing_dim.agg(F.max("customer_sk")).first()
    max_sk = max_sk_row[0] if max_sk_row[0] is not None else 0

    new_versions = (
        new_versions_needed
        .withColumn(
            "customer_sk",
            F.lit(max_sk) + F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy(business_key)
            )
        )
        .withColumn("effective_start_date", F.lit(as_of_date))
        .withColumn("effective_end_date", F.lit(FAR_FUTURE_DATE))
        .withColumn("is_current", F.lit(True))
    )

    result = (
        unchanged
        .unionByName(changed_old)
        .unionByName(missing_from_source)
        .unionByName(new_versions)
        .unionByName(historical_rows)
    )
    return result


# ---------------------------------------------------------------------
# I/O orchestration
# ---------------------------------------------------------------------

def run_dim_customer_scd2(as_of_date: date = None):
    spark = SparkSession.builder.appName("gold_dim_customer_scd2").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    as_of_date = as_of_date or date.today()

    source_df = spark.table(f"{SILVER_DB}.customers").select(
        BUSINESS_KEY, *TRACKED_COLUMNS, *PASSTHROUGH_COLUMNS
    )

    dim_table = f"{GOLD_DB}.dim_customer"
    if spark.catalog.tableExists(dim_table):
        existing_dim = spark.table(dim_table)
    else:
        existing_dim = None

    new_snapshot = compute_scd2_snapshot(existing_dim, source_df, as_of_date)
    new_snapshot.write.format("delta").mode("overwrite").saveAsTable(dim_table)
    print(f"[gold] dim_customer rebuilt: {new_snapshot.count()} total versioned rows")


def run_fact_sales():
    """Builds fact_sales joined against the CURRENT dim_customer version and dim_product."""
    spark = SparkSession.builder.appName("gold_fact_sales").getOrCreate()

    orders = spark.table(f"{SILVER_DB}.orders")
    order_items = spark.table(f"{SILVER_DB}.order_items")
    dim_customer_current = spark.table(f"{GOLD_DB}.dim_customer").filter(F.col("is_current") == True)  # noqa: E712

    fact_sales = (
        order_items.join(orders, "order_id")
        .join(
            dim_customer_current.select("customer_id", "customer_sk", "loyalty_tier"),
            "customer_id",
        )
        .select(
            "order_id", "order_item_id", "customer_sk", "product_id",
            "order_date", "order_status", "quantity", "unit_price",
            "discount_pct", "line_total", "loyalty_tier",
        )
    )
    fact_sales.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.fact_sales")
    print(f"[gold] fact_sales rebuilt: {fact_sales.count()} rows")


if __name__ == "__main__":
    run_dim_customer_scd2()
    run_fact_sales()
