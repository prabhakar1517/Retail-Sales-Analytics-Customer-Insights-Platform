"""
src/silver/customers_silver.py
------------------------------
Silver Layer customer deduplication and validation:
- Cleans and trims names, emails, phone, city, segment, loyalty tier.
- Validates email formats using regex: ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$
- Deduplicates customer records on customer_id:
    Uses Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc(), F.col("_ingest_timestamp").desc())
    Keep row_number == 1 (latest canonical version).
- Separate invalid records (null customer_id, malformed emails) to rejected/customers.
- Persists silver_customers and rejected/customers in Parquet.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("silver.customers")

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def process_customers_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_path = resolve_output_path("bronze_customers")
    silver_path = resolve_output_path("silver") + "/customers"
    reject_path = resolve_output_path("silver_rejected") + "/customers"

    df = spark.read.parquet(bronze_path)
    now_utc = datetime.now(timezone.utc)

    # Normalize column names from Oracle (uppercase) or CSV
    normalized_cols = {c: c.lower() for c in df.columns}
    for old_col, new_col in normalized_cols.items():
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)

    # Clean attributes
    cleaned_df = (
        df
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("name", F.initcap(F.trim(F.col("name"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("phone", F.trim(F.col("phone")))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("country", F.trim(F.col("country")))
        .withColumn("segment", F.upper(F.trim(F.col("segment"))))
        .withColumn("loyalty_tier", F.upper(F.trim(F.col("loyalty_tier"))))
        .withColumn("updated_at", F.col("updated_at").cast(TimestampType()))
    )

    # Email format validation regex
    is_valid_email = F.col("email").rlike(EMAIL_REGEX)
    is_valid_id = F.col("customer_id").isNotNull() & (F.col("customer_id") != "")

    is_valid = is_valid_id & is_valid_email

    valid_records = cleaned_df.filter(is_valid)
    rejected_records = (
        cleaned_df.filter(~is_valid)
        .withColumn(
            "reject_reason",
            F.when(~is_valid_id, "NULL_OR_EMPTY_CUSTOMER_ID")
            .when(~is_valid_email, "MALFORMED_EMAIL_ADDRESS")
            .otherwise("INVALID_CUSTOMER_RECORD")
        )
        .withColumn("_rejected_timestamp", F.lit(now_utc))
    )

    # Deduplication on customer_id using Window + row_number
    window_spec = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("_ingest_timestamp").desc_nulls_last()
    )

    deduped_df = (
        valid_records
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_cleaned_timestamp", F.lit(now_utc))
    )

    valid_cnt = deduped_df.count()
    reject_cnt = rejected_records.count()

    deduped_df.write.mode("overwrite").parquet(silver_path)
    if reject_cnt > 0:
        rejected_records.write.mode("overwrite").parquet(reject_path)

    logger.info("Customers Silver processed: %d unique valid, %d rejected.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt
