"""
src/gold/dim_product.py
-----------------------
Builds dim_product dimension table from silver_products:
- product_key: Surrogate key (monotonically_increasing_id + 1)
- product_id: Natural business key
- product_name
- category
- brand
- supplier_id: Link to supplier
- unit_price
- cost_price
- reorder_level
- is_active
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.dim_product")


def build_dim_product(spark: SparkSession) -> int:
    silver_path = resolve_output_path("silver") + "/products"
    dest_path = resolve_output_path("gold") + "/dim_product"

    silver_df = spark.read.parquet(silver_path)

    # Assign surrogate key
    dim_df = (
        silver_df
        .withColumn("product_key", (F.monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            "product_key",
            "product_id",
            "product_name",
            "category",
            "brand",
            "supplier_id",
            "unit_price",
            "cost_price",
            "reorder_level",
            "is_active"
        )
    )

    dim_df.write.mode("overwrite").parquet(dest_path)
    cnt = dim_df.count()
    logger.info("dim_product generated: %d records written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildDimProduct")
    try:
        build_dim_product(spark)
    finally:
        stop_spark(spark)
