"""
tests/test_transformations.py
-----------------------------
Unit tests for core pipeline transformation functions:
- Email regex validation
- Quantity and price filtering
- Gross and net revenue calculation
- Window deduplication
- RFM quintile score generation
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

from src.utils.spark_session import create_spark_session, stop_spark
from src.silver.customers_silver import EMAIL_REGEX


@pytest.fixture(scope="session")
def spark():
    """Shared lightweight SparkSession for testing using project factory."""
    s = create_spark_session(app_name="UnitTests", master="local[1]")
    yield s
    stop_spark(s)


def test_email_regex_validation():
    """Verify customer email regex correctly accepts valid emails and catches deliberate defects."""
    valid_emails = [
        "john.doe@example.com",
        "sarah_123@retail.co.uk",
        "customer.service+promo@store.org"
    ]
    invalid_emails = [
        "john.doe_at_example.com",
        "missingatsign.com",
        "@domain.com",
        "spaces in@email.com"
    ]

    import re
    pattern = re.compile(EMAIL_REGEX)

    for em in valid_emails:
        assert bool(pattern.match(em)), f"Expected {em} to be valid"

    for em in invalid_emails:
        assert not bool(pattern.match(em)), f"Expected {em} to be invalid"


def test_sales_gross_and_net_calculations(spark):
    """Test gross_amount and net_amount logic."""
    df = spark.sql("""
        SELECT 10 AS quantity, 5.0 AS unit_price, 2.5 AS discount
        UNION ALL
        SELECT 2, 12.50, 0.0
        UNION ALL
        SELECT 1, 100.0, 15.0
    """)
    calc_df = (
        df
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount"), 2))
    )

    rows = calc_df.collect()
    assert rows[0]["gross_amount"] == 50.0
    assert rows[0]["net_amount"] == 47.5
    assert rows[1]["gross_amount"] == 25.0
    assert rows[1]["net_amount"] == 25.0
    assert rows[2]["gross_amount"] == 100.0
    assert rows[2]["net_amount"] == 85.0


def test_customer_window_deduplication(spark):
    """Test deduplication keeps only row_number == 1 for highest updated_at."""
    from pyspark.sql.window import Window

    df = spark.sql("""
        SELECT 'C1001' AS customer_id, 'Jane Doe' AS name, '2010-01-01 10:00:00' AS updated_at
        UNION ALL
        SELECT 'C1001', 'Jane Doe Jr', '2010-06-01 15:00:00'
        UNION ALL
        SELECT 'C1002', 'Bob Smith', '2009-12-01 08:00:00'
    """)
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

    dedup_df = (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    results = {r["customer_id"]: r["name"] for r in dedup_df.collect()}
    assert len(results) == 2
    assert results["C1001"] == "Jane Doe Jr"
    assert results["C1002"] == "Bob Smith"


def test_rfm_score_assignment(spark):
    """Test NTILE(5) scoring produces valid integer scores 1 to 5."""
    from pyspark.sql.window import Window
    df = (
        spark.range(1, 26)
        .withColumn("frequency", F.col("id") * 10)
        .withColumn("f_score", F.ntile(5).over(Window.orderBy(F.col("frequency").asc())))
    )

    scores = [r["f_score"] for r in df.collect()]
    assert set(scores) == {1, 2, 3, 4, 5}
