"""
src/ingestion/mysql_ingestor.py
-------------------------------
Ingests MySQL master tables using PySpark JDBC:
- products
- stores
- suppliers
- promotions

Features:
- Connects via MySQL Connector/J driver (com.mysql.cj.jdbc.Driver).
- Appends lineage audit columns (_ingest_timestamp, _source_system, _batch_id).
- Stores clean, raw Parquet in individual Bronze layer directories.
- Gracefully falls back to generated CSV backups if MySQL unreachable.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.config_loader import get_config, get_path, resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("ingestion.mysql_ingestor")


def ingest_mysql_tables(spark: SparkSession, batch_id: str) -> Dict[str, int]:
    """
    Ingest MySQL master tables into Bronze layer.
    Returns dictionary mapping table_name -> count_ingested.
    """
    cfg = get_config()
    mysql_cfg = cfg["mysql"]
    tables = mysql_cfg.get("tables", ["products", "stores", "suppliers", "promotions"])
    now_utc = datetime.now(timezone.utc)
    results = {}

    for table in tables:
        bronze_dest_key = f"bronze_{table}"
        bronze_path = resolve_output_path(bronze_dest_key)
        logger.info("Ingesting MySQL master table: %s via JDBC...", table)

        df = None
        try:
            df = (
                spark.read
                .format("jdbc")
                .option("url", mysql_cfg["jdbc_url"])
                .option("driver", mysql_cfg["jdbc_driver"])
                .option("dbtable", table)
                .option("user", mysql_cfg["user"])
                .option("password", mysql_cfg["password"])
                .option("fetchsize", mysql_cfg.get("fetch_size", 5000))
                .load()
            )
            row_count = df.count()
            logger.info("Read %d records from MySQL table: %s via JDBC.", row_count, table)
        except Exception as exc:
            logger.warning("JDBC read for MySQL table %s failed (%s). Checking CSV backup...", table, exc)
            csv_backup = get_path("generated") / "masters" / f"{table}.csv"
            if csv_backup.exists():
                df = (
                    spark.read
                    .option("header", "true")
                    .csv(str(csv_backup))
                )
                row_count = df.count()
                logger.info("Loaded %d rows for %s from CSV backup.", row_count, table)
            else:
                raise RuntimeError(f"Could not load table {table} from MySQL or backup {csv_backup}") from exc

        if df is not None:
            enriched_df = (
                df
                .withColumn("_ingest_timestamp", F.lit(now_utc))
                .withColumn("_source_system", F.lit(f"MYSQL_{table.upper()}"))
                .withColumn("_batch_id", F.lit(batch_id))
            )

            (
                enriched_df.write
                .mode("overwrite")
                .parquet(bronze_path)
            )
            logger.info("Bronze table %s written to: %s", table, bronze_path)
            results[table] = row_count

    return results
