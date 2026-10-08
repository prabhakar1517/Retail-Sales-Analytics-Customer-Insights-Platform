"""
src/gold/customer_rfm.py
------------------------
Calculates analytical Customer RFM (Recency, Frequency, Monetary) Segmentation mart:
- Recency: Days since customer's last purchase relative to reference snapshot date.
- Frequency: Distinct number of orders (sale_ids) made by the customer.
- Monetary: Total net spend across all purchases.
- Scores (1 to 5):
    Computed using NTILE(5) window functions across the customer base.
- Combined RFM Score:
    Concatenated string e.g., '555', '421'.
- Customer Segment:
    Categorizes customers based on R and FM averages:
    - 'Champions': R in (4,5) and F in (4,5)
    - 'Loyal Customers': F in (3,4,5) and M in (3,4,5)
    - 'Potential Loyalists': R in (3,4,5) and F in (1,2)
    - 'At Risk': R in (1,2) and F in (3,4,5)
    - 'Lost Customers': R in (1,2) and F in (1,2)
    - 'Promising / Needs Attention': Others
"""
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.customer_rfm")


def build_customer_rfm(spark: SparkSession) -> int:
    silver_sales_path = resolve_output_path("silver") + "/sales"
    dest_path = resolve_output_path("gold") + "/customer_rfm"

    sales_df = spark.read.parquet(silver_sales_path)

    # Filter out guest purchases without customer_id
    valid_cust_sales = sales_df.filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))

    # Reference snapshot date = day after maximum sale date in dataset
    max_date_row = valid_cust_sales.select(F.max("sale_date")).collect()
    snapshot_date = max_date_row[0][0] if max_date_row and max_date_row[0][0] else datetime(2011, 12, 31)

    logger.info("Computing RFM metrics using reference snapshot date: %s", snapshot_date)

    # 1. Aggregate per customer
    rfm_agg = (
        valid_cust_sales
        .groupBy("customer_id")
        .agg(
            F.max("sale_date").alias("last_order_date"),
            F.countDistinct("sale_id").alias("frequency"),
            F.round(F.sum("net_amount"), 2).alias("monetary")
        )
        .withColumn(
            "recency",
            F.datediff(F.lit(snapshot_date), F.col("last_order_date"))
        )
    )

    # 2. Compute 1-5 Quintile Scores using NTILE
    # Recency: lower days = better score (descending datediff gets lower score, ascending gets higher score)
    recency_window = Window.orderBy(F.col("recency").desc())
    freq_window = Window.orderBy(F.col("frequency").asc())
    monetary_window = Window.orderBy(F.col("monetary").asc())

    scored_df = (
        rfm_agg
        .withColumn("r_score", F.ntile(5).over(recency_window))
        .withColumn("f_score", F.ntile(5).over(freq_window))
        .withColumn("m_score", F.ntile(5).over(monetary_window))
        .withColumn(
            "rfm_score",
            F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score"))
        )
    )

    # 3. Classify Segments
    segmented_df = (
        scored_df
        .withColumn(
            "rfm_segment",
            F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champions")
            .when((F.col("f_score") >= 3) & (F.col("m_score") >= 3), "Loyal Customers")
            .when((F.col("r_score") >= 3) & (F.col("f_score") <= 2), "Potential Loyalists")
            .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "At Risk")
            .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Lost Customers")
            .otherwise("Needs Attention")
        )
        .withColumn("_calculated_at", F.current_timestamp())
    )

    segmented_df.write.mode("overwrite").parquet(dest_path)
    cnt = segmented_df.count()
    logger.info("customer_rfm mart built: %d customers scored and segmented to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildCustomerRFM")
    try:
        build_customer_rfm(spark)
    finally:
        stop_spark(spark)
