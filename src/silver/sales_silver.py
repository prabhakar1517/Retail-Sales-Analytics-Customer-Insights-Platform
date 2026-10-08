"""
src/silver/sales_silver.py
--------------------------
Silver Layer Sales Validation and Cleaning:
1. Deduplication on sale_id using Window + row_number() over ingest_timestamp.
2. Type casting: quantity -> INT, unit_price -> DOUBLE, discount -> DOUBLE, sale_date -> TIMESTAMP.
3. Rule-based validation:
   - Mandatory IDs: sale_id, store_id, product_id must be non-null and non-empty.
   - Quantity > 0.
   - Unit price > 0.
   - Valid parseable sale_date.
   - Foreign key validation against silver_products and silver_stores.
4. Business calculations:
   - gross_amount = quantity * unit_price
   - net_amount = gross_amount - discount
5. Separation of rejected records with explicit pipe-separated reject_reason codes:
   - REJECT_DUPLICATE_SALE_ID
   - REJECT_NULL_PRODUCT_OR_STORE
   - REJECT_INVALID_QUANTITY
   - REJECT_INVALID_PRICE
   - REJECT_INVALID_DATE
   - REJECT_UNKNOWN_PRODUCT_FK
   - REJECT_UNKNOWN_STORE_FK
6. Writes clean sales to silver/sales and rejected to silver/rejected/sales in Parquet format.
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

logger = get_logger("silver.sales")


def process_sales_silver(spark: SparkSession) -> Tuple[int, int]:
    bronze_sales_path = resolve_output_path("bronze_sales")
    silver_products_path = resolve_output_path("silver") + "/products"
    silver_stores_path = resolve_output_path("silver") + "/stores"
    silver_sales_path = resolve_output_path("silver") + "/sales"
    reject_sales_path = resolve_output_path("silver_rejected") + "/sales"

    raw_sales_df = spark.read.parquet(bronze_sales_path)
    products_df = spark.read.parquet(silver_products_path).select("product_id").distinct()
    stores_df = spark.read.parquet(silver_stores_path).select("store_id").distinct()

    now_utc = datetime.now(timezone.utc)

    # 1. Type casting & preliminary sanitization
    parsed_df = (
        raw_sales_df
        .withColumn("sale_id", F.trim(F.col("sale_id")))
        .withColumn("store_id", F.upper(F.trim(F.col("store_id"))))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("promo_id", F.upper(F.trim(F.col("promo_id"))))
        .withColumn("payment_mode", F.upper(F.trim(F.col("payment_mode"))))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.round(F.col("unit_price").cast(DoubleType()), 2))
        .withColumn("discount", F.round(F.coalesce(F.col("discount").cast(DoubleType()), F.lit(0.0)), 2))
        .withColumn(
            "parsed_sale_date",
            F.coalesce(
                F.to_timestamp(F.col("sale_date"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("sale_date"), "yyyy-MM-dd")
            )
        )
    )

    # 2. Duplicate Detection on sale_id
    window_spec = Window.partitionBy("sale_id").orderBy(
        F.col("_ingest_timestamp").desc_nulls_last()
    )
    ranked_df = parsed_df.withColumn("dup_rank", F.row_number().over(window_spec))

    # 3. Foreign Key Lookups
    joined_df = (
        ranked_df
        .join(products_df.withColumn("valid_prod_fk", F.lit(1)), on="product_id", how="left")
        .join(stores_df.withColumn("valid_store_fk", F.lit(1)), on="store_id", how="left")
    )

    # 4. Reason Code Tagging
    flagged_df = (
        joined_df
        .withColumn("c_dup", F.when(F.col("dup_rank") > 1, "DUPLICATE_SALE_ID").otherwise(None))
        .withColumn("c_id", F.when(F.col("sale_id").isNull() | (F.col("sale_id") == ""), "MISSING_SALE_ID").otherwise(None))
        .withColumn("c_prod", F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "MISSING_PRODUCT_ID").otherwise(None))
        .withColumn("c_store", F.when(F.col("store_id").isNull() | (F.col("store_id") == ""), "MISSING_STORE_ID").otherwise(None))
        .withColumn("c_qty", F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY").otherwise(None))
        .withColumn("c_price", F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0.0), "INVALID_UNIT_PRICE").otherwise(None))
        .withColumn("c_date", F.when(F.col("parsed_sale_date").isNull(), "UNPARSEABLE_SALE_DATE").otherwise(None))
        .withColumn("c_fk_prod", F.when(F.col("valid_prod_fk").isNull(), "UNKNOWN_PRODUCT_FK").otherwise(None))
        .withColumn("c_fk_store", F.when(F.col("valid_store_fk").isNull(), "UNKNOWN_STORE_FK").otherwise(None))
        .withColumn(
            "reject_reason",
            F.concat_ws(
                " | ",
                F.col("c_dup"), F.col("c_id"), F.col("c_prod"), F.col("c_store"),
                F.col("c_qty"), F.col("c_price"), F.col("c_date"),
                F.col("c_fk_prod"), F.col("c_fk_store")
            )
        )
    )

    is_valid = F.col("reject_reason") == ""

    # 5. Clean Valid Dataset
    valid_sales_df = (
        flagged_df
        .filter(is_valid)
        .withColumn("sale_date", F.col("parsed_sale_date"))
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount"), 2))
        .withColumn("_cleaned_timestamp", F.lit(now_utc))
        .drop("parsed_sale_date", "dup_rank", "valid_prod_fk", "valid_store_fk",
              "c_dup", "c_id", "c_prod", "c_store", "c_qty", "c_price", "c_date", "c_fk_prod", "c_fk_store", "reject_reason")
    )

    # 6. Rejected Dataset
    rejected_sales_df = (
        flagged_df
        .filter(~is_valid)
        .withColumn("_rejected_timestamp", F.lit(now_utc))
        .drop("parsed_sale_date", "dup_rank", "valid_prod_fk", "valid_store_fk",
              "c_dup", "c_id", "c_prod", "c_store", "c_qty", "c_price", "c_date", "c_fk_prod", "c_fk_store")
    )

    valid_cnt = valid_sales_df.count()
    reject_cnt = rejected_sales_df.count()

    valid_sales_df.write.mode("overwrite").parquet(silver_sales_path)
    if reject_cnt > 0:
        rejected_sales_df.write.mode("overwrite").parquet(reject_sales_path)

    logger.info("Sales Silver complete: %d valid records, %d rejected records.", valid_cnt, reject_cnt)
    return valid_cnt, reject_cnt
