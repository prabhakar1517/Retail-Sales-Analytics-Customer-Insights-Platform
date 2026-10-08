"""
src/gold/fact_returns.py
------------------------
Builds fact_returns in Star Schema:
- Surrogate keys attached:
    return_date_key (INT)
    product_key (INT)
    customer_key (INT, retrieved from associated sale_id if available)
- Measures:
    quantity
    refund_amount
- Attributes:
    return_id
    sale_id
    reason
- Partitioned by return_date_key for querying performance.
"""
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.fact_returns")


def build_fact_returns(spark: SparkSession) -> int:
    silver_returns_path = resolve_output_path("silver") + "/returns"
    gold_dir = resolve_output_path("gold")
    dest_path = gold_dir + "/fact_returns"

    returns_df = spark.read.parquet(silver_returns_path)
    dim_prod = spark.read.parquet(gold_dir + "/dim_product").select("product_key", "product_id")

    # Add integer return_date_key
    enriched_returns = returns_df.withColumn(
        "return_date_key",
        F.date_format(F.col("return_date"), "yyyyMMdd").cast(IntegerType())
    )

    # Join Product Dimension
    j1 = enriched_returns.join(dim_prod, on="product_id", how="left")

    fact_df = (
        j1
        .withColumn("product_key", F.coalesce(F.col("product_key"), F.lit(0)))
        .select(
            "return_id",
            "sale_id",
            "return_date_key",
            "product_key",
            "return_date",
            "product_id",
            "quantity",
            "refund_amount",
            "reason"
        )
    )

    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("return_date_key")
        .parquet(dest_path)
    )

    cnt = fact_df.count()
    logger.info("fact_returns generated: %d records partitioned by return_date_key written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildFactReturns")
    try:
        build_fact_returns(spark)
    finally:
        stop_spark(spark)
