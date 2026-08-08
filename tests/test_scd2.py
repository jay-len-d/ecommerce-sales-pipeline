import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "transformations"))

from gold_scd2 import (  # noqa: E402
    add_row_hash,
    initialize_dimension,
    compute_scd2_snapshot,
    TRACKED_COLUMNS,
    FAR_FUTURE_DATE,
)

CUSTOMER_COLS = ["customer_id", "loyalty_tier", "address", "city", "state",
                  "postal_code", "email", "first_name", "last_name", "country"]


def make_customer_row(customer_id, loyalty_tier="BRONZE", address="1 Main St", city="Austin",
                       state="TX", postal_code="78701", email="x@example.com",
                       first_name="Jane", last_name="Doe", country="USA"):
    return (customer_id, loyalty_tier, address, city, state, postal_code, email,
            first_name, last_name, country)


# ---------------------------------------------------------------------
# add_row_hash
# ---------------------------------------------------------------------

def test_add_row_hash_same_input_same_hash(spark):
    df1 = spark.createDataFrame([make_customer_row(1)], CUSTOMER_COLS)
    df2 = spark.createDataFrame([make_customer_row(1)], CUSTOMER_COLS)

    h1 = add_row_hash(df1, TRACKED_COLUMNS).collect()[0]["row_hash"]
    h2 = add_row_hash(df2, TRACKED_COLUMNS).collect()[0]["row_hash"]
    assert h1 == h2


def test_add_row_hash_changes_when_tracked_column_changes(spark):
    df1 = spark.createDataFrame([make_customer_row(1, loyalty_tier="BRONZE")], CUSTOMER_COLS)
    df2 = spark.createDataFrame([make_customer_row(1, loyalty_tier="GOLD")], CUSTOMER_COLS)

    h1 = add_row_hash(df1, TRACKED_COLUMNS).collect()[0]["row_hash"]
    h2 = add_row_hash(df2, TRACKED_COLUMNS).collect()[0]["row_hash"]
    assert h1 != h2


def test_add_row_hash_unchanged_when_untracked_column_changes(spark):
    """first_name is NOT in TRACKED_COLUMNS, so changing it must not change the hash."""
    df1 = spark.createDataFrame([make_customer_row(1, first_name="Jane")], CUSTOMER_COLS)
    df2 = spark.createDataFrame([make_customer_row(1, first_name="Janet")], CUSTOMER_COLS)

    h1 = add_row_hash(df1, TRACKED_COLUMNS).collect()[0]["row_hash"]
    h2 = add_row_hash(df2, TRACKED_COLUMNS).collect()[0]["row_hash"]
    assert h1 == h2


# ---------------------------------------------------------------------
# initialize_dimension (bootstrap)
# ---------------------------------------------------------------------

def test_initialize_dimension_sets_all_rows_current(spark):
    df = spark.createDataFrame(
        [make_customer_row(1), make_customer_row(2)], CUSTOMER_COLS
    )
    result = initialize_dimension(df, TRACKED_COLUMNS, date(2026, 1, 1)).collect()

    assert len(result) == 2
    for row in result:
        assert row["is_current"] is True
        assert row["effective_start_date"] == date(2026, 1, 1)
        assert row["effective_end_date"] == FAR_FUTURE_DATE

    sks = sorted(r["customer_sk"] for r in result)
    assert sks == [1, 2], "Surrogate keys should be assigned sequentially starting at 1"


# ---------------------------------------------------------------------
# compute_scd2_snapshot -- the core SCD2 behavior
# ---------------------------------------------------------------------

def test_scd2_bootstrap_when_no_existing_dimension(spark):
    source = spark.createDataFrame([make_customer_row(1)], CUSTOMER_COLS)
    result = compute_scd2_snapshot(None, source, date(2026, 1, 1)).collect()

    assert len(result) == 1
    assert result[0]["is_current"] is True


def test_scd2_unchanged_customer_is_carried_forward_without_new_version(spark):
    source = spark.createDataFrame([make_customer_row(1, loyalty_tier="GOLD")], CUSTOMER_COLS)
    dim_v1 = compute_scd2_snapshot(None, source, date(2026, 1, 1))

    # Re-run with an IDENTICAL source snapshot on a later date
    source_same = spark.createDataFrame([make_customer_row(1, loyalty_tier="GOLD")], CUSTOMER_COLS)
    dim_v2 = compute_scd2_snapshot(dim_v1, source_same, date(2026, 2, 1)).collect()

    assert len(dim_v2) == 1, "No change in tracked attributes should not create a new version"
    assert dim_v2[0]["effective_start_date"] == date(2026, 1, 1), "Original start date preserved"
    assert dim_v2[0]["is_current"] is True


def test_scd2_changed_attribute_creates_new_version_and_expires_old(spark):
    source_v1 = spark.createDataFrame([make_customer_row(1, loyalty_tier="BRONZE")], CUSTOMER_COLS)
    dim_v1 = compute_scd2_snapshot(None, source_v1, date(2026, 1, 1))

    source_v2 = spark.createDataFrame([make_customer_row(1, loyalty_tier="GOLD")], CUSTOMER_COLS)
    dim_v2 = compute_scd2_snapshot(dim_v1, source_v2, date(2026, 3, 1)).collect()

    assert len(dim_v2) == 2, "A tracked attribute change should produce exactly 2 versions"

    old_version = next(r for r in dim_v2 if r["is_current"] is False)
    new_version = next(r for r in dim_v2 if r["is_current"] is True)

    assert old_version["loyalty_tier"] == "BRONZE"
    assert old_version["effective_start_date"] == date(2026, 1, 1)
    assert old_version["effective_end_date"] == date(2026, 2, 28), "Expires the day before new version starts"

    assert new_version["loyalty_tier"] == "GOLD"
    assert new_version["effective_start_date"] == date(2026, 3, 1)
    assert new_version["effective_end_date"] == FAR_FUTURE_DATE

    assert new_version["customer_sk"] != old_version["customer_sk"], \
        "New version must get a new surrogate key"


def test_scd2_untracked_attribute_change_does_not_create_new_version(spark):
    source_v1 = spark.createDataFrame([make_customer_row(1, first_name="Jane")], CUSTOMER_COLS)
    dim_v1 = compute_scd2_snapshot(None, source_v1, date(2026, 1, 1))

    # Only first_name changes -- not a tracked column
    source_v2 = spark.createDataFrame([make_customer_row(1, first_name="Janet")], CUSTOMER_COLS)
    dim_v2 = compute_scd2_snapshot(dim_v1, source_v2, date(2026, 2, 1)).collect()

    assert len(dim_v2) == 1, "Untracked attribute change should not version the dimension"


def test_scd2_brand_new_customer_is_inserted_as_current(spark):
    source_v1 = spark.createDataFrame([make_customer_row(1)], CUSTOMER_COLS)
    dim_v1 = compute_scd2_snapshot(None, source_v1, date(2026, 1, 1))

    source_v2 = spark.createDataFrame(
        [make_customer_row(1), make_customer_row(2)], CUSTOMER_COLS
    )
    dim_v2 = compute_scd2_snapshot(dim_v1, source_v2, date(2026, 2, 1)).collect()

    assert len(dim_v2) == 2
    new_cust = next(r for r in dim_v2 if r["customer_id"] == 2)
    assert new_cust["is_current"] is True
    assert new_cust["effective_start_date"] == date(2026, 2, 1)


def test_scd2_customer_missing_from_source_is_preserved_unchanged(spark):
    """Customers absent from a later extract (e.g. extraction gap) are NOT auto-expired."""
    source_v1 = spark.createDataFrame(
        [make_customer_row(1), make_customer_row(2)], CUSTOMER_COLS
    )
    dim_v1 = compute_scd2_snapshot(None, source_v1, date(2026, 1, 1))

    source_v2 = spark.createDataFrame([make_customer_row(1)], CUSTOMER_COLS)  # customer 2 missing
    dim_v2 = compute_scd2_snapshot(dim_v1, source_v2, date(2026, 2, 1)).collect()

    assert len(dim_v2) == 2
    cust2 = next(r for r in dim_v2 if r["customer_id"] == 2)
    assert cust2["is_current"] is True, "Missing-from-source should not implicitly expire the record"


def test_scd2_full_history_accumulates_across_multiple_changes(spark):
    """Three successive loyalty_tier changes should yield 3 total versions, 1 current."""
    source_v1 = spark.createDataFrame([make_customer_row(1, loyalty_tier="BRONZE")], CUSTOMER_COLS)
    dim = compute_scd2_snapshot(None, source_v1, date(2026, 1, 1))

    source_v2 = spark.createDataFrame([make_customer_row(1, loyalty_tier="SILVER")], CUSTOMER_COLS)
    dim = compute_scd2_snapshot(dim, source_v2, date(2026, 2, 1))

    source_v3 = spark.createDataFrame([make_customer_row(1, loyalty_tier="GOLD")], CUSTOMER_COLS)
    dim = compute_scd2_snapshot(dim, source_v3, date(2026, 3, 1))

    result = dim.orderBy("effective_start_date").collect()
    assert len(result) == 3
    assert [r["loyalty_tier"] for r in result] == ["BRONZE", "SILVER", "GOLD"]
    assert [r["is_current"] for r in result] == [False, False, True]
    # surrogate keys must all be distinct across the full history
    assert len({r["customer_sk"] for r in result}) == 3
