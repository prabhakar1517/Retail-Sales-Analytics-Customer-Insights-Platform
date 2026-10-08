"""
src/gold/dim_customer_scd2.py
-----------------------------
Builds dim_customer with Slowly Changing Dimension Type 2 (SCD2):
- customer_key: Surrogate key (monotonically_increasing_id + 1)
- customer_id: Natural business key
- name, email, phone, city, country
- segment, loyalty_tier (tracked attributes)
- effective_date: TIMESTAMP when record became active
- expiry_date: TIMESTAMP when record expired (or '9999-12-31 00:00:00')
- is_current: 1 for current active record, 0 for historical version

Uses Window functions to compute historical vs current versions based on updated_at.
"""
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, TimestampType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.dim_customer")


def build_dim_customer_scd2(spark: SparkSession) -> int:
    silver_path = resolve_output_path("silver") + "/customers"
    dest_path = resolve_output_path("gold") + "/dim_customer"

    silver_df = spark.read.parquet(silver_path)

    # Window partitioned by natural key to calculate lead update time for SCD2
    window_spec = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").asc_nulls_first(),
        F.col("_ingest_timestamp").asc_nulls_first()
    )

    scd2_df = (
        silver_df
        .withColumn(
            "effective_date",
            F.coalesce(F.col("updated_at"), F.col("_ingest_timestamp"))
        )
        .withColumn(
            "next_effective_date",
            F.lead("effective_date").over(window_spec)
        )
        .withColumn(
            "expiry_date",
            F.when(
                F.col("next_effective_date").isNotNull(),
                F.col("next_effective_date")
            ).otherwise(F.to_timestamp(F.lit("9999-12-31 00:00:00")))
        )
        .withColumn(
            "is_current",
            F.when(F.col("next_effective_date").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "customer_key",
            (F.monotonically_increasing_id() + 1).cast(IntegerType())
        )
        .select(
            "customer_key",
            "customer_id",
            "name",
            "email",
            "phone",
            "city",
            "country",
            "segment",
            "loyalty_tier",
            "effective_date",
            "expiry_date",
            "is_current"
        )
    )

    scd2_df.write.mode("overwrite").parquet(dest_path)
    cnt = scd2_df.count()
    current_cnt = scd2_df.filter(F.col("is_current") == 1).count()
    logger.info("dim_customer (SCD2) built: %d total rows (%d active current) written to %s", cnt, current_cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildDimCustomerSCD2")
    try:
        build_dim_customer_scd2(spark)
    finally:
        stop_spark(spark)
