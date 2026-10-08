"""
src/gold/gold_pipeline.py
-------------------------
Master orchestrator for the Gold Layer:
1. Builds Dimensions:
   - dim_date (calendar date dimension with keys and statutory flags)
   - dim_product (surrogate product_key)
   - dim_store (surrogate store_key)
   - dim_supplier (surrogate supplier_key)
   - dim_promotion (surrogate promo_key)
   - dim_customer (SCD Type 2 with effective_date, expiry_date, is_current)
2. Builds Facts:
   - fact_sales (Star schema with surrogate keys, partitioned by date_key)
   - fact_returns (Star schema with surrogate keys, partitioned by return_date_key)
3. Builds Analytical Mart:
   - customer_rfm (Recency, Frequency, Monetary NTILE scores and segment tags)
4. Logs summary verification metrics across all Gold tables.
"""
from datetime import datetime, timezone
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from src.utils.spark_session import create_spark_session, stop_spark
from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger
from src.gold.dim_date import build_dim_date
from src.gold.dim_product import build_dim_product
from src.gold.dim_store import build_dim_store
from src.gold.dim_supplier import build_dim_supplier
from src.gold.dim_promotion import build_dim_promotion
from src.gold.dim_customer_scd2 import build_dim_customer_scd2
from src.gold.fact_sales import build_fact_sales
from src.gold.fact_returns import build_fact_returns
from src.gold.customer_rfm import build_customer_rfm

logger = get_logger("gold.pipeline")


def run_gold_pipeline() -> dict:
    batch_id = datetime.now(timezone.utc).strftime("GOLD_BATCH_%Y%m%d_%H%M%S")
    logger.info("==================================================")
    logger.info(" Starting Gold Star Schema Pipeline: %s", batch_id)
    logger.info("==================================================")

    spark = create_spark_session(app_name="GoldPipeline")
    results = {}

    try:
        # 1. Dimensions
        logger.info("--- 1. Building Dimension Tables ---")
        results["dim_date"] = build_dim_date(spark)
        results["dim_product"] = build_dim_product(spark)
        results["dim_store"] = build_dim_store(spark)
        results["dim_supplier"] = build_dim_supplier(spark)
        results["dim_promotion"] = build_dim_promotion(spark)
        results["dim_customer"] = build_dim_customer_scd2(spark)

        # 2. Facts
        logger.info("--- 2. Building Fact Tables ---")
        results["fact_sales"] = build_fact_sales(spark)
        results["fact_returns"] = build_fact_returns(spark)

        # 3. Customer RFM Mart
        logger.info("--- 3. Building Customer RFM Analytical Mart ---")
        results["customer_rfm"] = build_customer_rfm(spark)

        # 4. Summary Output
        logger.info("==================================================")
        logger.info(" GOLD LAYER GENERATION SUMMARY")
        logger.info("==================================================")
        for tbl, cnt in results.items():
            logger.info("  -> Table: %-16s | Total Records: %-8d", tbl, cnt)
        logger.info("==================================================")
        logger.info(" Gold Pipeline Completed Successfully!")
        logger.info("==================================================")
        return results

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    run_gold_pipeline()
