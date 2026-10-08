"""
src/ingestion/oracle_ingestor.py
--------------------------------
Ingests CRM.CUSTOMERS from Oracle XE using PySpark JDBC.
Features:
- Connects via Oracle JDBC driver (ojdbc11).
- Incremental read using watermark on UPDATED_AT.
- Falls back to local CSV backup if Oracle container unreachable.
- Appends lineage audit columns (_ingest_timestamp, _source_system, _batch_id).
- Writes raw Parquet to Bronze Customers layer.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.ingestion.schemas import ORACLE_CUSTOMERS_SCHEMA
from src.utils.config_loader import get_config, get_path, resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("ingestion.oracle_ingestor")


def ingest_oracle_customers(spark: SparkSession, batch_id: str, watermark: Optional[str] = None) -> int:
    """
    Read Oracle XE CRM.CUSTOMERS table via JDBC and store in Bronze layer.
    Returns row count ingested.
    """
    cfg = get_config()
    ora_cfg = cfg["oracle"]
    bronze_customers_path = resolve_output_path("bronze_customers")
    now_utc = datetime.now(timezone.utc)

    # Build query with optional incremental watermark filter
    table_query = ora_cfg.get("table", "CUSTOMERS")
    if watermark:
        table_query = f"(SELECT * FROM {table_query} WHERE UPDATED_AT > TO_TIMESTAMP('{watermark}', 'YYYY-MM-DD HH24:MI:SS')) custom_src"

    logger.info("Reading Oracle CRM customers via JDBC from %s...", ora_cfg["jdbc_url"])
    df = None
    try:
        df = (
            spark.read
            .format("jdbc")
            .option("url", ora_cfg["jdbc_url"])
            .option("driver", ora_cfg["jdbc_driver"])
            .option("dbtable", table_query)
            .option("user", ora_cfg["user"])
            .option("password", ora_cfg["password"])
            .option("fetchsize", ora_cfg.get("fetch_size", 5000))
            .load()
        )
        row_count = df.count()
        logger.info("Successfully read %d rows from Oracle XE via JDBC.", row_count)
    except Exception as exc:
        logger.warning("JDBC read from Oracle failed (%s). Falling back to CSV backup...", exc)
        csv_backup = get_path("generated") / "masters" / "crm_customers.csv"
        if csv_backup.exists():
            df = (
                spark.read
                .option("header", "true")
                .csv(str(csv_backup))
            )
            row_count = df.count()
            logger.info("Loaded %d rows from Oracle CRM CSV backup.", row_count)
        else:
            raise RuntimeError(f"Oracle ingestion failed and no backup found at {csv_backup}") from exc

    if df is not None:
        enriched_df = (
            df
            .withColumn("_ingest_timestamp", F.lit(now_utc))
            .withColumn("_source_system", F.lit("ORACLE_CRM"))
            .withColumn("_batch_id", F.lit(batch_id))
        )

        (
            enriched_df.write
            .mode("overwrite")
            .parquet(bronze_customers_path)
        )
        logger.info("Bronze customers written to: %s", bronze_customers_path)
        return row_count

    return 0
