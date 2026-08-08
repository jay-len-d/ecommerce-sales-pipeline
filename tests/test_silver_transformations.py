import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "transformations"))

from silver import clean_customers, clean_orders, calculate_line_total, quarantine_invalid  # noqa: E402


def test_clean_customers_deduplicates_keeping_latest(spark):
    data = [
        (1, "jane", "DOE", "Jane@Example.com", "BRONZE", datetime(2026, 1, 1)),
        (1, "Jane", "Doe", "jane@example.com", "GOLD", datetime(2026, 3, 1)),  # newer -> should win
        (2, "bob", "smith", "bob@example.com", "silver", datetime(2026, 1, 1)),
    ]
    cols = ["customer_id", "first_name", "last_name", "email", "loyalty_tier", "updated_at"]
    df = spark.createDataFrame(data, cols)

    result = clean_customers(df).orderBy("customer_id").collect()

    assert len(result) == 2, "Expected duplicate customer_id=1 rows to be deduplicated to 1"
    cust1 = result[0]
    assert cust1["loyalty_tier"] == "GOLD", "Should keep the most recently updated version"
    assert cust1["first_name"] == "Jane"
    assert cust1["last_name"] == "Doe"


def test_clean_customers_standardizes_name_casing(spark):
    data = [(1, "jOHN", "sMITH", "john@example.com", "bronze", datetime(2026, 1, 1))]
    cols = ["customer_id", "first_name", "last_name", "email", "loyalty_tier", "updated_at"]
    df = spark.createDataFrame(data, cols)

    result = clean_customers(df).collect()[0]
    assert result["first_name"] == "John"
    assert result["last_name"] == "Smith"


def test_clean_customers_invalid_loyalty_tier_defaults_to_bronze(spark):
    data = [(1, "Ann", "Lee", "ann@example.com", "NOT_A_REAL_TIER", datetime(2026, 1, 1))]
    cols = ["customer_id", "first_name", "last_name", "email", "loyalty_tier", "updated_at"]
    df = spark.createDataFrame(data, cols)

    result = clean_customers(df).collect()[0]
    assert result["loyalty_tier"] == "BRONZE"


def test_clean_customers_flags_invalid_email_without_dropping(spark):
    data = [
        (1, "Ann", "Lee", "not-an-email", "GOLD", datetime(2026, 1, 1)),
        (2, "Bob", "Ray", "bob@example.com", "GOLD", datetime(2026, 1, 1)),
    ]
    cols = ["customer_id", "first_name", "last_name", "email", "loyalty_tier", "updated_at"]
    df = spark.createDataFrame(data, cols)

    result = {r["customer_id"]: r for r in clean_customers(df).collect()}
    assert result[1]["is_email_valid"] is False
    assert result[2]["is_email_valid"] is True
    assert len(result) == 2, "Invalid email should be flagged, not dropped"


def test_clean_orders_flags_null_customer_id_as_invalid(spark):
    data = [
        (100, 1, "placed", 5.00, datetime(2026, 1, 1)),
        (101, None, "placed", 5.00, datetime(2026, 1, 1)),
    ]
    cols = ["order_id", "customer_id", "order_status", "shipping_cost", "updated_at"]
    df = spark.createDataFrame(data, cols)

    result = clean_orders(df)
    valid, quarantined = quarantine_invalid(result)

    assert valid.count() == 1
    assert quarantined.count() == 1
    assert quarantined.collect()[0]["order_id"] == 101


def test_clean_orders_deduplicates_keeping_latest_status(spark):
    data = [
        (100, 1, "placed", 5.00, datetime(2026, 1, 1)),
        (100, 1, "shipped", 5.00, datetime(2026, 1, 5)),  # newer -> should win
    ]
    cols = ["order_id", "customer_id", "order_status", "shipping_cost", "updated_at"]
    df = spark.createDataFrame(data, cols)

    result = clean_orders(df).collect()
    assert len(result) == 1
    assert result[0]["order_status"] == "SHIPPED"


def test_calculate_line_total_applies_discount_correctly(spark):
    data = [(1, 1, 1, 2, 100.00, 10.0)]  # qty=2, unit_price=100, 10% discount -> 180.00
    cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"]
    df = spark.createDataFrame(data, cols)

    result = calculate_line_total(df).collect()[0]
    assert result["line_total"] == 180.00


def test_calculate_line_total_zero_discount(spark):
    data = [(1, 1, 1, 3, 50.00, 0.0)]  # qty=3, unit_price=50, no discount -> 150.00
    cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"]
    df = spark.createDataFrame(data, cols)

    result = calculate_line_total(df).collect()[0]
    assert result["line_total"] == 150.00
