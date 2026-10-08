"""
src/ingestion/csv_ingestor.py
-----------------------------
Ingests daily Sales and Returns CSV files into Bronze Parquet.
Features:
- Enforces explicit StructType schemas (no inferSchema).
- Handles CSV escaping and multiLine options.
- Appends audit/lineage metadata columns:
    _ingest_timestamp: Current UTC timestamp
    _source_file: Original filename
    _batch_id: Execution run ID
- Incremental file tracking using SQLite file tracker: skips previously processed files.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.ingestion.schemas import SALES_RAW_SCHEMA, RETURNS_RAW_SCHEMA
from src.utils.config_loader import get_config, get_path, resolve_output_path
from src.utils.file_tracker import is_processed, mark_processed
from src.utils.logger import get_logger

logger = get_logger("ingestion.csv_ingestor")


def ingest_sales_csv(spark: SparkSession, batch_id: str) -> Tuple[int, int]:
    """
    Ingest unprocessed daily sales CSV files to Bronze sales parquet.
    Returns (num_files_processed, total_rows_ingested).
    """
    sales_dir = get_path("generated_sales")
    sales_files = sorted(list(sales_dir.glob("sales_*.csv")))
    
    files_to_process = [f for f in sales_files if not is_processed(str(f))]
    if not files_to_process:
        logger.info("No new sales CSV files to ingest.")
        return 0, 0

    bronze_sales_path = resolve_output_path("bronze_sales")
    now_utc = datetime.now(timezone.utc)
    total_rows = 0

    for file_path in files_to_process:
        logger.info("Ingesting sales file: %s", file_path.name)
        df: DataFrame = (
            spark.read
            .option("header", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("multiLine", "true")
            .schema(SALES_RAW_SCHEMA)
            .csv(str(file_path))
        )

        row_count = df.count()
        if row_count > 0:
            enriched_df = (
                df
                .withColumn("_ingest_timestamp", F.lit(now_utc))
                .withColumn("_source_file", F.lit(file_path.name))
                .withColumn("_batch_id", F.lit(batch_id))
            )

            (
                enriched_df.write
                .mode("append")
                .parquet(bronze_sales_path)
            )

        mark_processed(
            file_path=str(file_path),
            source_type="sales_csv",
            batch_id=batch_id,
            row_count=row_count,
            status="SUCCESS"
        )
        total_rows += row_count

    logger.info("Sales CSV ingestion complete: %d files, %d rows.", len(files_to_process), total_rows)
    return len(files_to_process), total_rows


def ingest_returns_csv(spark: SparkSession, batch_id: str) -> Tuple[int, int]:
    """
    Ingest unprocessed daily returns CSV files to Bronze returns parquet.
    Returns (num_files_processed, total_rows_ingested).
    """
    returns_dir = get_path("generated_returns")
    returns_files = sorted(list(returns_dir.glob("returns_*.csv")))
    
    files_to_process = [f for f in returns_files if not is_processed(str(f))]
    if not files_to_process:
        logger.info("No new returns CSV files to ingest.")
        return 0, 0

    bronze_returns_path = resolve_output_path("bronze_returns")
    now_utc = datetime.now(timezone.utc)
    total_rows = 0

    for file_path in files_to_process:
        logger.info("Ingesting returns file: %s", file_path.name)
        df: DataFrame = (
            spark.read
            .option("header", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .schema(RETURNS_RAW_SCHEMA)
            .csv(str(file_path))
        )

        row_count = df.count()
        if row_count > 0:
            enriched_df = (
                df
                .withColumn("_ingest_timestamp", F.lit(now_utc))
                .withColumn("_source_file", F.lit(file_path.name))
                .withColumn("_batch_id", F.lit(batch_id))
            )

            (
                enriched_df.write
                .mode("append")
                .parquet(bronze_returns_path)
            )

        mark_processed(
            file_path=str(file_path),
            source_type="returns_csv",
            batch_id=batch_id,
            row_count=row_count,
            status="SUCCESS"
        )
        total_rows += row_count

    logger.info("Returns CSV ingestion complete: %d files, %d rows.", len(files_to_process), total_rows)
    return len(files_to_process), total_rows
