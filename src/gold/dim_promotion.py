"""
src/gold/dim_promotion.py
-------------------------
Builds dim_promotion dimension table from silver_promotions:
- promo_key: Surrogate key (monotonically_increasing_id + 1)
- promo_id: Natural key
- promo_name
- promo_type
- discount_type
- discount_value
- start_date
- end_date
- min_order_amount
- is_active
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.dim_promotion")


def build_dim_promotion(spark: SparkSession) -> int:
    silver_path = resolve_output_path("silver") + "/promotions"
    dest_path = resolve_output_path("gold") + "/dim_promotion"

    silver_df = spark.read.parquet(silver_path)

    dim_df = (
        silver_df
        .withColumn("promo_key", (F.monotonically_increasing_id() + 1).cast(IntegerType()))
        .select(
            "promo_key",
            "promo_id",
            "promo_name",
            "promo_type",
            "discount_type",
            "discount_value",
            "start_date",
            "end_date",
            "min_order_amount",
            "is_active"
        )
    )

    dim_df.write.mode("overwrite").parquet(dest_path)
    cnt = dim_df.count()
    logger.info("dim_promotion generated: %d records written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildDimPromotion")
    try:
        build_dim_promotion(spark)
    finally:
        stop_spark(spark)
