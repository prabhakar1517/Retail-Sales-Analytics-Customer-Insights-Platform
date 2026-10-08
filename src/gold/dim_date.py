"""
src/gold/dim_date.py
--------------------
Generates dim_date dimension table covering all transaction dates:
Date range: 2009-01-01 to 2012-12-31 (configured in config.yaml).
Attributes:
- date_key (YYYYMMDD integer format, primary key)
- full_date (DATE format)
- day_of_week (1-7, Mon-Sun)
- day_name (Monday, Tuesday...)
- day_of_month (1-31)
- day_of_year (1-366)
- week_of_year (1-53)
- month_number (1-12)
- month_name (January, February...)
- month_year (Dec 2009)
- quarter_number (1-4)
- quarter_name (Q1, Q2...)
- year_number (2009, 2010...)
- is_weekend (0 or 1)
- is_holiday (0 or 1, UK bank holidays approximation)
"""
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DateType, StringType
)

from src.utils.config_loader import get_config, resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.dim_date")


def build_dim_date(spark: SparkSession) -> int:
    cfg = get_config()
    gold_cfg = cfg.get("gold", {})
    start_str = gold_cfg.get("dim_date_start", "2009-01-01")
    end_str = gold_cfg.get("dim_date_end", "2012-12-31")

    dest_path = resolve_output_path("gold") + "/dim_date"
    start_dt = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_str, "%Y-%m-%d").date()

    delta = (end_dt - start_dt).days + 1
    logger.info("Generating dim_date from %s to %s (%d days)...", start_str, end_str, delta)

    # Native Spark SQL generation (no Python worker serialization required)
    df = (
        spark.range(delta)
        .select(
            F.date_add(F.to_date(F.lit(start_str)), F.col("id").cast("int")).alias("full_date")
        )
        .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("day_of_week", F.dayofweek(F.col("full_date")))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("day_of_month", F.dayofmonth(F.col("full_date")))
        .withColumn("day_of_year", F.dayofyear(F.col("full_date")))
        .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
        .withColumn("month_number", F.month(F.col("full_date")))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("month_year", F.date_format(F.col("full_date"), "MMM yyyy"))
        .withColumn("quarter_number", F.quarter(F.col("full_date")))
        .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter(F.col("full_date"))))
        .withColumn("year_number", F.year(F.col("full_date")))
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1, 7), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "is_holiday",
            F.when(
                ((F.col("month_number") == 12) & (F.col("day_of_month").isin(25, 26))) |
                ((F.col("month_number") == 1) & (F.col("day_of_month") == 1)),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .select(
            "date_key", "full_date", "day_of_week", "day_name", "day_of_month",
            "day_of_year", "week_of_year", "month_number", "month_name", "month_year",
            "quarter_number", "quarter_name", "year_number", "is_weekend", "is_holiday"
        )
    )

    df.write.mode("overwrite").parquet(dest_path)

    cnt = df.count()
    logger.info("dim_date generated successfully: %d rows written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildDimDate")
    try:
        build_dim_date(spark)
    finally:
        stop_spark(spark)
