"""
src/bronze/bronze_pipeline.py
-----------------------------
Master orchestrator for the Bronze Layer:
1. Initializes SparkSession with JDBC drivers and memory configuration.
2. Ingests daily sales CSV files (incremental).
3. Ingests daily returns CSV files (incremental).
4. Ingests Oracle CRM Customers via JDBC.
5. Ingests MySQL master tables (products, stores, suppliers, promotions) via JDBC.
6. Performs reconciliation audit of all Bronze tables and logs output.
"""
from datetime import datetime, timezone
import sys
from pathlib import Path

from src.utils.spark_session import create_spark_session, stop_spark
from src.utils.config_loader import get_config, resolve_output_path
from src.utils.logger import get_logger
from src.ingestion.csv_ingestor import ingest_sales_csv, ingest_returns_csv
from src.ingestion.oracle_ingestor import ingest_oracle_customers
from src.ingestion.mysql_ingestor import ingest_mysql_tables

logger = get_logger("bronze.pipeline")


def run_bronze_pipeline() -> dict:
    batch_id = datetime.now(timezone.utc).strftime("BATCH_%Y%m%d_%H%M%S")
    logger.info("==================================================")
    logger.info(" Starting Bronze Pipeline Run: %s", batch_id)
    logger.info("==================================================")

    spark = create_spark_session(app_name="BronzePipeline")
    results = {"batch_id": batch_id, "tables": {}}

    try:
        # 1. Sales CSV Ingestion
        s_files, s_rows = ingest_sales_csv(spark, batch_id)
        results["tables"]["bronze_sales"] = s_rows

        # 2. Returns CSV Ingestion
        r_files, r_rows = ingest_returns_csv(spark, batch_id)
        results["tables"]["bronze_returns"] = r_rows

        # 3. Oracle CRM Ingestion
        ora_rows = ingest_oracle_customers(spark, batch_id)
        results["tables"]["bronze_customers"] = ora_rows

        # 4. MySQL Masters Ingestion
        mysql_counts = ingest_mysql_tables(spark, batch_id)
        for tbl, cnt in mysql_counts.items():
            results["tables"][f"bronze_{tbl}"] = cnt

        # 5. Read back from Bronze Parquet to verify written data
        logger.info("--------------------------------------------------")
        logger.info(" Bronze Layer Verification (Parquet Reads):")
        logger.info("--------------------------------------------------")
        for key in ["sales", "returns", "customers", "products", "stores", "suppliers", "promotions"]:
            b_path = resolve_output_path(f"bronze_{key}")
            try:
                b_df = spark.read.parquet(b_path)
                cnt = b_df.count()
                cols = len(b_df.columns)
                logger.info("  -> Table: bronze_%-12s | Count: %-8d | Columns: %d", key, cnt, cols)
            except Exception as e:
                logger.warning("  -> Table: bronze_%-12s | Verification failed: %s", key, e)

        logger.info("==================================================")
        logger.info(" Bronze Pipeline Completed Successfully!")
        logger.info("==================================================")
        return results

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    run_bronze_pipeline()
