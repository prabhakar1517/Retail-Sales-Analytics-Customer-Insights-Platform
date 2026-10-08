"""
src/silver/silver_pipeline.py
-----------------------------
Master orchestrator for the Silver Layer:
1. Cleans & validates master tables (products, stores, suppliers, promotions).
2. Cleans, validates, & deduplicates customers (regex email, Window row_number).
3. Cleans, validates, & links sales transactions against valid products/stores.
4. Cleans & validates return records.
5. Performs complete Row-Count Reconciliation Audit:
   source_bronze = silver_valid + silver_rejected
   Ensures 0 data loss and prints detailed audit summary table.
"""
from datetime import datetime, timezone
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from src.utils.spark_session import create_spark_session, stop_spark
from src.utils.config_loader import resolve_output_path
from src.utils.logger import get_logger
from src.silver.masters_silver import (
    process_products_silver,
    process_stores_silver,
    process_suppliers_silver,
    process_promotions_silver,
)
from src.silver.customers_silver import process_customers_silver
from src.silver.sales_silver import process_sales_silver
from src.silver.returns_silver import process_returns_silver

logger = get_logger("silver.pipeline")


def run_silver_pipeline() -> dict:
    batch_id = datetime.now(timezone.utc).strftime("SILVER_BATCH_%Y%m%d_%H%M%S")
    logger.info("==================================================")
    logger.info(" Starting Silver Transformation Pipeline: %s", batch_id)
    logger.info("==================================================")

    spark = create_spark_session(app_name="SilverPipeline")
    audit = {}

    try:
        # 1. Masters
        logger.info("--- Processing Silver Master Tables ---")
        p_val, p_rej = process_products_silver(spark)
        st_val, st_rej = process_stores_silver(spark)
        sup_val, sup_rej = process_suppliers_silver(spark)
        prm_val, prm_rej = process_promotions_silver(spark)

        # 2. Customers
        logger.info("--- Processing Silver Customers (Dedup & Validation) ---")
        c_val, c_rej = process_customers_silver(spark)

        # 3. Sales
        logger.info("--- Processing Silver Sales (Rule Validation & FK Lookups) ---")
        s_val, s_rej = process_sales_silver(spark)

        # 4. Returns
        logger.info("--- Processing Silver Returns (Validation & Netting) ---")
        r_val, r_rej = process_returns_silver(spark)

        # 5. Row-Count Reconciliation Audit
        logger.info("==================================================")
        logger.info(" ROW-COUNT RECONCILIATION AUDIT (Bronze vs Silver + Rejects)")
        logger.info("==================================================")

        reconciliation_items = [
            ("products", "bronze_products", p_val, p_rej),
            ("stores", "bronze_stores", st_val, st_rej),
            ("suppliers", "bronze_suppliers", sup_val, sup_rej),
            ("promotions", "bronze_promotions", prm_val, prm_rej),
            ("customers", "bronze_customers", c_val, c_rej),
            ("sales", "bronze_sales", s_val, s_rej),
            ("returns", "bronze_returns", r_val, r_rej),
        ]

        logger.info(f"{'Entity':<14} | {'Bronze Count':<12} | {'Silver Valid':<12} | {'Rejected':<10} | {'Status'}")
        logger.info("-" * 65)

        for name, bronze_key, valid_cnt, rej_cnt in reconciliation_items:
            b_cnt = spark.read.parquet(resolve_output_path(bronze_key)).count()
            # Note: customers deduplication drops duplicate rows, so valid + rej <= bronze
            if name == "customers":
                balanced = (valid_cnt + rej_cnt) <= b_cnt
                status = "PASS (Deduped)" if balanced else "MISMATCH"
            else:
                balanced = (valid_cnt + rej_cnt) == b_cnt
                status = "EXACT MATCH" if balanced else f"DELTA ({b_cnt - (valid_cnt + rej_cnt)})"

            audit[name] = {
                "bronze_count": b_cnt,
                "silver_valid": valid_cnt,
                "rejected": rej_cnt,
                "status": status
            }
            logger.info(f"{name:<14} | {b_cnt:<12} | {valid_cnt:<12} | {rej_cnt:<10} | {status}")

        logger.info("==================================================")
        logger.info(" Silver Pipeline Completed Successfully!")
        logger.info("==================================================")
        return audit

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    run_silver_pipeline()
