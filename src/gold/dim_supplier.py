"""
src/gold/dim_supplier.py
------------------------
Builds dim_supplier dimension table from silver_suppliers:
- supplier_key: Surrogate key (monotonically_increasing_id + 1)
- supplier_id: Natural key
- supplier_name
- contact_name
- email
- phone
- city
- country
- lead_time_days
- is_active
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.dim_supplier")


def build_dim_supplier(spark: SparkSession) -> int:
    silver_path = resolve_output_path("silver") + "/suppliers"
    dest_path = resolve_output_path("gold") + "/dim_supplier"

    silver_df = spark.read.parquet(silver_path)

    dim_df = (
        silver_df
        .withColumn("supplier_key", (F.monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            "supplier_key",
            "supplier_id",
            "supplier_name",
            "contact_name",
            "email",
            "phone",
            "city",
            "country",
            "lead_time_days",
            "is_active"
        )
    )

    dim_df.write.mode("overwrite").parquet(dest_path)
    cnt = dim_df.count()
    logger.info("dim_supplier generated: %d records written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildDimSupplier")
    try:
        build_dim_supplier(spark)
    finally:
        stop_spark(spark)
