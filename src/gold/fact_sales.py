"""
src/gold/fact_sales.py
----------------------
Builds fact_sales in Star Schema:
- Joins silver_sales with:
    dim_date on sale_date_key = date_key (YYYYMMDD)
    dim_product on product_id
    dim_store on store_id
    dim_promotion on promo_id
    dim_customer (SCD2 join: customer_id and sale_timestamp between effective_date and expiry_date)
- Surrogate keys attached:
    date_key (INT)
    product_key (INT)
    store_key (INT)
    promo_key (INT)
    customer_key (INT)
- Measures:
    quantity
    unit_price
    gross_amount
    discount
    net_amount
- Partitioned by sale_date_key (or sale_date) for analytical and Redshift loading efficiency.
"""
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("gold.fact_sales")


def build_fact_sales(spark: SparkSession) -> int:
    silver_sales_path = resolve_output_path("silver") + "/sales"
    gold_dir = resolve_output_path("gold")
    dest_path = gold_dir + "/fact_sales"

    sales_df = spark.read.parquet(silver_sales_path)
    dim_prod = spark.read.parquet(gold_dir + "/dim_product").select("product_key", "product_id")
    dim_store = spark.read.parquet(gold_dir + "/dim_store").select("store_key", "store_id")
    dim_promo = spark.read.parquet(gold_dir + "/dim_promotion").select("promo_key", "promo_id")
    dim_cust = spark.read.parquet(gold_dir + "/dim_customer")

    # Add integer date_key (YYYYMMDD)
    enriched_sales = sales_df.withColumn(
        "date_key",
        F.date_format(F.col("sale_date"), "yyyyMMdd").cast(IntegerType())
    )

    # 1. Join Product Dimension
    j1 = enriched_sales.join(dim_prod, on="product_id", how="left")

    # 2. Join Store Dimension
    j2 = j1.join(dim_store, on="store_id", how="left")

    # 3. Join Promotion Dimension
    j3 = j2.join(dim_promo, on="promo_id", how="left")

    # 4. Join Customer SCD2 Dimension (effective_date <= sale_date < expiry_date)
    # Default to unknown customer_key = 0 for guest purchases (null customer_id)
    cust_join_cond = (
        (j3.customer_id == dim_cust.customer_id) &
        (j3.sale_date >= dim_cust.effective_date) &
        (j3.sale_date < dim_cust.expiry_date)
    )
    j4 = j3.join(dim_cust.select("customer_key", "customer_id", "effective_date", "expiry_date"), on=cust_join_cond, how="left")

    fact_df = (
        j4
        .withColumn("product_key", F.coalesce(F.col("product_key"), F.lit(0)))
        .withColumn("store_key", F.coalesce(F.col("store_key"), F.lit(0)))
        .withColumn("promo_key", F.coalesce(F.col("promo_key"), F.lit(0)))
        .withColumn("customer_key", F.coalesce(dim_cust.customer_key, F.lit(0)))
        .select(
            "sale_id",
            "date_key",
            "product_key",
            "store_key",
            "promo_key",
            "customer_key",
            "sale_date",
            j3["customer_id"].alias("customer_id"),
            j3["product_id"].alias("product_id"),
            j3["store_id"].alias("store_id"),
            "quantity",
            "unit_price",
            "gross_amount",
            "discount",
            "net_amount",
            "payment_mode"
        )
    )

    (
        fact_df.write
        .mode("overwrite")
        .partitionBy("date_key")
        .parquet(dest_path)
    )

    cnt = fact_df.count()
    logger.info("fact_sales generated: %d records partitioned by date_key written to %s", cnt, dest_path)
    return cnt


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(app_name="BuildFactSales")
    try:
        build_fact_sales(spark)
    finally:
        stop_spark(spark)
