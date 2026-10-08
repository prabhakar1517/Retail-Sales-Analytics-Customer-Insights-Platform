"""
src/silver/masters_silver.py
----------------------------
Silver Layer cleaning & standardization for Master tables:
1. silver_products:
   - Trim text fields, uppercase product_id and category.
   - Cast unit_price, cost_price to Decimal(10, 2), reorder_level to int.
   - Filter valid unit_price > 0 and cost_price > 0.
   - Separate invalid records to rejected/products with reason code.
2. silver_stores:
   - Trim text fields, standardize store_type and region.
   - Validate square_footage > 0.
   - Separate invalid records to rejected/stores.
3. silver_suppliers:
   - Standardize supplier_name, validate lead_time_days > 0.
4. silver_promotions:
   - Validate start_date <= end_date, discount_value >= 0.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("silver.masters")


def process_products_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_path = resolve_output_path("bronze_products")
    silver_path = resolve_output_path("silver") + "/products"
    reject_path = resolve_output_path("silver_rejected") + "/products"

    df = spark.read.parquet(bronze_path)
    now_utc = datetime.now(timezone.utc)

    # Clean & cast
    cleaned_df = (
        df
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("supplier_id", F.upper(F.trim(F.col("supplier_id"))))
        .withColumn("unit_price", F.round(F.col("unit_price").cast("double"), 2))
        .withColumn("cost_price", F.round(F.col("cost_price").cast("double"), 2))
        .withColumn("reorder_level", F.col("reorder_level").cast(IntegerType()))
    )

    # Validation rules
    is_valid = (
        F.col("product_id").isNotNull() & (F.col("product_id") != "") &
        F.col("unit_price").isNotNull() & (F.col("unit_price") > 0) &
        F.col("cost_price").isNotNull() & (F.col("cost_price") > 0)
    )

    valid_df = (
        cleaned_df.filter(is_valid)
        .withColumn("_cleaned_timestamp", F.lit(now_utc))
    )

    rejected_df = (
        cleaned_df.filter(~is_valid)
        .withColumn(
            "reject_reason",
            F.concat_ws(
                " | ",
                F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "NULL_OR_EMPTY_PRODUCT_ID"),
                F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "INVALID_UNIT_PRICE"),
                F.when(F.col("cost_price").isNull() | (F.col("cost_price") <= 0), "INVALID_COST_PRICE"),
            )
        )
        .withColumn("_rejected_timestamp", F.lit(now_utc))
    )

    valid_cnt = valid_df.count()
    reject_cnt = rejected_df.count()

    valid_df.write.mode("overwrite").parquet(silver_path)
    if reject_cnt > 0:
        rejected_df.write.mode("overwrite").parquet(reject_path)

    logger.info("Products Silver processed: %d valid, %d rejected.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt


def process_stores_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_path = resolve_output_path("bronze_stores")
    silver_path = resolve_output_path("silver") + "/stores"
    reject_path = resolve_output_path("silver_rejected") + "/stores"

    df = spark.read.parquet(bronze_path)
    now_utc = datetime.now(timezone.utc)

    cleaned_df = (
        df
        .withColumn("store_id", F.upper(F.trim(F.col("store_id"))))
        .withColumn("store_name", F.trim(F.col("store_name")))
        .withColumn("store_type", F.upper(F.trim(F.col("store_type"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("country", F.trim(F.col("country")))
        .withColumn("square_footage", F.col("square_footage").cast(IntegerType()))
    )

    is_valid = (
        F.col("store_id").isNotNull() & (F.col("store_id") != "") &
        F.col("square_footage").isNotNull() & (F.col("square_footage") > 0)
    )

    valid_df = cleaned_df.filter(is_valid).withColumn("_cleaned_timestamp", F.lit(now_utc))
    rejected_df = (
        cleaned_df.filter(~is_valid)
        .withColumn("reject_reason", F.lit("INVALID_STORE_ID_OR_SQUARE_FOOTAGE"))
        .withColumn("_rejected_timestamp", F.lit(now_utc))
    )

    valid_cnt = valid_df.count()
    reject_cnt = rejected_df.count()

    valid_df.write.mode("overwrite").parquet(silver_path)
    if reject_cnt > 0:
        rejected_df.write.mode("overwrite").parquet(reject_path)

    logger.info("Stores Silver processed: %d valid, %d rejected.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt


def process_suppliers_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_path = resolve_output_path("bronze_suppliers")
    silver_path = resolve_output_path("silver") + "/suppliers"
    reject_path = resolve_output_path("silver_rejected") + "/suppliers"

    df = spark.read.parquet(bronze_path)
    now_utc = datetime.now(timezone.utc)

    cleaned_df = (
        df
        .withColumn("supplier_id", F.upper(F.trim(F.col("supplier_id"))))
        .withColumn("supplier_name", F.trim(F.col("supplier_name")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("lead_time_days", F.col("lead_time_days").cast(IntegerType()))
    )

    is_valid = (
        F.col("supplier_id").isNotNull() & (F.col("supplier_id") != "") &
        F.col("lead_time_days").isNotNull() & (F.col("lead_time_days") > 0)
    )

    valid_df = cleaned_df.filter(is_valid).withColumn("_cleaned_timestamp", F.lit(now_utc))
    rejected_df = (
        cleaned_df.filter(~is_valid)
        .withColumn("reject_reason", F.lit("INVALID_SUPPLIER_ID_OR_LEAD_TIME"))
        .withColumn("_rejected_timestamp", F.lit(now_utc))
    )

    valid_cnt = valid_df.count()
    reject_cnt = rejected_df.count()

    valid_df.write.mode("overwrite").parquet(silver_path)
    if reject_cnt > 0:
        rejected_df.write.mode("overwrite").parquet(reject_path)

    logger.info("Suppliers Silver processed: %d valid, %d rejected.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt


def process_promotions_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_path = resolve_output_path("bronze_promotions")
    silver_path = resolve_output_path("silver") + "/promotions"
    reject_path = resolve_output_path("silver_rejected") + "/promotions"

    df = spark.read.parquet(bronze_path)
    now_utc = datetime.now(timezone.utc)

    cleaned_df = (
        df
        .withColumn("promo_id", F.upper(F.trim(F.col("promo_id"))))
        .withColumn("promo_name", F.trim(F.col("promo_name")))
        .withColumn("discount_value", F.round(F.col("discount_value").cast("double"), 2))
        .withColumn("start_date", F.col("start_date").cast(DateType()))
        .withColumn("end_date", F.col("end_date").cast(DateType()))
    )

    is_valid = (
        F.col("promo_id").isNotNull() & (F.col("promo_id") != "") &
        F.col("discount_value").isNotNull() & (F.col("discount_value") >= 0) &
        (F.col("start_date") <= F.col("end_date"))
    )

    valid_df = cleaned_df.filter(is_valid).withColumn("_cleaned_timestamp", F.lit(now_utc))
    rejected_df = (
        cleaned_df.filter(~is_valid)
        .withColumn(
            "reject_reason",
            F.when(F.col("start_date") > F.col("end_date"), "START_DATE_AFTER_END_DATE")
            .otherwise("INVALID_PROMO_DATA")
        )
        .withColumn("_rejected_timestamp", F.lit(now_utc))
    )

    valid_cnt = valid_df.count()
    reject_cnt = rejected_df.count()

    valid_df.write.mode("overwrite").parquet(silver_path)
    if reject_cnt > 0:
        rejected_df.write.mode("overwrite").parquet(reject_path)

    logger.info("Promotions Silver processed: %d valid, %d rejected.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt
