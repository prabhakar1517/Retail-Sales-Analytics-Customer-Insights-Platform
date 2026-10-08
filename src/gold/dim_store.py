"""
src/gold/dim_store.py
---------------------
Builds dim_store dimension table from silver_stores:
- store_key: Surrogate key (monotonically_increasing_id + 1)
- store_id: Natural key
- store_name
- store_type
- city
- region
- country
- manager_name
- phone
- square_footage
- opened_date
- is_active
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.dim_store")


def build_dim_store(spark: SparkSession) -> int:
    silver_path = resolve_output_path("silver") + "/stores"
    dest_path = resolve_output_path("gold") + "/dim_store"

    silver_df = spark.read.parquet(silver_path)

    dim_df = (
        silver_df
        .withColumn("store_key", (F.monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            "store_key",
            "store_id",
            "store_name",
            "store_type",
            "city",
            "region",
            "country",
            "manager_name",
            "phone",
            "square_footage",
            "opened_date",
            "is_active"
        )
    )

    dim_df.write.mode("overwrite").parquet(dest_path)
    cnt = dim_df.count()
    logger.info("dim_store generated: %d records written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildDimStore")
    try:
        build_dim_store(spark)
    finally:
        stop_spark(spark)
