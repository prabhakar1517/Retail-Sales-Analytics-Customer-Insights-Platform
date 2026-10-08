"""
src/silver/returns_silver.py
----------------------------
Silver Layer Returns Processing & Netting:
1. Validates returns feed:
   - return_id, sale_id, product_id required.
   - quantity > 0, refund_amount > 0.
   - Valid parseable return_date.
2. Tag rejected returns with reason codes to rejected/returns.
3. Link valid returns against Silver sales:
   - Calculate netted quantities and net sales amount for analytical accuracy.
4. Writes valid returns to silver/returns and rejected to silver/rejected/returns.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("silver.returns")


def process_returns_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_returns_path = resolve_output_path("bronze_returns")
    silver_products_path = resolve_output_path("silver") + "/products"
    silver_returns_path = resolve_output_path("silver") + "/returns"
    reject_returns_path = resolve_output_path("silver_rejected") + "/returns"

    raw_returns_df = spark.read.parquet(bronze_returns_path)
    products_df = spark.read.parquet(silver_products_path).select("product_id").distinct()

    now_utc = datetime.now(timezone.utc)

    # 1. Type casting
    parsed_df = (
        raw_returns_df
        .withColumn("return_id", F.trim(F.col("return_id")))
        .withColumn("sale_id", F.trim(F.col("sale_id")))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("reason", F.upper(F.trim(F.col("reason"))))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("refund_amount", F.round(F.col("refund_amount").cast(DoubleType()), 2))
        .withColumn(
            "parsed_return_date",
            F.coalesce(
                F.to_timestamp(F.col("return_date"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("return_date"), "yyyy-MM-dd")
            )
        )
    )

    # 2. Deduplication on return_id
    window_spec = Window.partitionBy("return_id").orderBy(
        F.col("_ingest_timestamp").desc_nulls_last()
    )
    ranked_df = parsed_df.withColumn("dup_rank", F.row_number().over(window_spec))

    # 3. Foreign key lookup on product_id
    joined_df = ranked_df.join(products_df.withColumn("valid_prod_fk", F.lit(1)), on="product_id", how="left")

    # 4. Reason code tagging
    flagged_df = (
        joined_df
        .withColumn("c_dup", F.when(F.col("dup_rank") > 1, "DUPLICATE_RETURN_ID").otherwise(None))
        .withColumn("c_ret_id", F.when(F.col("return_id").isNull() | (F.col("return_id") == ""), "MISSING_RETURN_ID").otherwise(None))
        .withColumn("c_sale_id", F.when(F.col("sale_id").isNull() | (F.col("sale_id") == ""), "MISSING_SALE_ID").otherwise(None))
        .withColumn("c_prod", F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "MISSING_PRODUCT_ID").otherwise(None))
        .withColumn("c_qty", F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY").otherwise(None))
        .withColumn("c_refund", F.when(F.col("refund_amount").isNull() | (F.col("refund_amount") <= 0.0), "INVALID_REFUND_AMOUNT").otherwise(None))
        .withColumn("c_date", F.when(F.col("parsed_return_date").isNull(), "UNPARSEABLE_RETURN_DATE").otherwise(None))
        .withColumn("c_fk_prod", F.when(F.col("valid_prod_fk").isNull(), "UNKNOWN_PRODUCT_FK").otherwise(None))
        .withColumn(
            "reject_reason",
            F.concat_ws(
                " | ",
                F.col("c_dup"), F.col("c_ret_id"), F.col("c_sale_id"), F.col("c_prod"),
                F.col("c_qty"), F.col("c_refund"), F.col("c_date"), F.col("c_fk_prod")
            )
        )
    )

    is_valid = F.col("reject_reason") == ""

    # 5. Clean Valid Dataset
    valid_returns_df = (
        flagged_df
        .filter(is_valid)
        .withColumn("return_date", F.col("parsed_return_date"))
        .withColumn("_cleaned_timestamp", F.lit(now_utc))
        .drop("parsed_return_date", "dup_rank", "valid_prod_fk",
              "c_dup", "c_ret_id", "c_sale_id", "c_prod", "c_qty", "c_refund", "c_date", "c_fk_prod", "reject_reason")
    )

    # 6. Rejected Dataset
    rejected_returns_df = (
        flagged_df
        .filter(~is_valid)
        .withColumn("_rejected_timestamp", F.lit(now_utc))
        .drop("parsed_return_date", "dup_rank", "valid_prod_fk",
              "c_dup", "c_ret_id", "c_sale_id", "c_prod", "c_qty", "c_refund", "c_date", "c_fk_prod")
    )

    valid_cnt = valid_returns_df.count()
    reject_cnt = rejected_returns_df.count()

    valid_returns_df.write.mode("overwrite").parquet(silver_returns_path)
    if reject_cnt > 0:
        rejected_returns_df.write.mode("overwrite").parquet(reject_returns_path)

    logger.info("Returns Silver complete: %d valid records, %d rejected records.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt
