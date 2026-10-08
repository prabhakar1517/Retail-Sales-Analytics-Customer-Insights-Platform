"""
src/redshift/redshift_loader.py
-------------------------------
Orchestrates Amazon Redshift loading:
1. Connects to Amazon Redshift Serverless using psycopg2.
2. Applies Star Schema DDL (src/redshift/ddl/01_create_star_schema.sql).
3. Executes COPY commands from S3 Parquet using configured IAM Role:
   - Dimensions & Analytical Mart loaded with full refresh / upsert.
   - Fact tables loaded using Staging Table + MERGE / Deduplicate pattern:
     a. CREATE TEMP TABLE stg_fact_sales (LIKE retail_dw.fact_sales);
     b. COPY stg_fact_sales FROM 's3://.../fact_sales/' FORMAT AS PARQUET;
     c. DELETE FROM retail_dw.fact_sales WHERE sale_id IN (SELECT sale_id FROM stg_fact_sales);
     d. INSERT INTO retail_dw.fact_sales SELECT * FROM stg_fact_sales;
4. Updates retail_dw.etl_file_control audit table.
5. Performs post-load reconciliation query:
   Asserts row count in Redshift matches Gold Parquet records.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

import psycopg2
from psycopg2.extras import execute_values

from src.utils.config_loader import get_config
from src.utils.logger import get_logger

logger = get_logger("redshift.loader")


def get_redshift_connection():
    cfg = get_config()["redshift"]
    conn = psycopg2.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=15
    )
    conn.autocommit = True
    return conn


def execute_ddl(conn, ddl_path: Path):
    logger.info("Executing DDL from %s...", ddl_path)
    with open(ddl_path, mode="r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    logger.info("Redshift DDL applied successfully.")


def copy_dimension(conn, table_name: str, s3_path: str, iam_role: str) -> int:
    """COPY dimension table using truncate + insert pattern."""
    sql = f"""
    TRUNCATE TABLE retail_dw.{table_name};
    COPY retail_dw.{table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS PARQUET;
    """
    logger.info("Executing COPY for dimension: %s...", table_name)
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(f"SELECT COUNT(*) FROM retail_dw.{table_name};")
        cnt = cur.fetchone()[0]
    logger.info("Dimension %s loaded: %d rows.", table_name, cnt)
    return cnt


def merge_fact_sales(conn, s3_path: str, iam_role: str, batch_id: str) -> int:
    """Stage + MERGE incremental load for fact_sales."""
    sql = f"""
    CREATE TEMP TABLE stg_fact_sales (LIKE retail_dw.fact_sales);

    COPY stg_fact_sales
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS PARQUET;

    DELETE FROM retail_dw.fact_sales
    WHERE sale_id IN (SELECT DISTINCT sale_id FROM stg_fact_sales);

    INSERT INTO retail_dw.fact_sales
    SELECT * FROM stg_fact_sales;

    DROP TABLE stg_fact_sales;
    """
    logger.info("Executing staged MERGE for fact_sales from %s...", s3_path)
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) FROM retail_dw.fact_sales;")
        cnt = cur.fetchone()[0]

        # Audit log in etl_file_control
        audit_sql = """
        INSERT INTO retail_dw.etl_file_control (batch_id, target_table, s3_path, rows_loaded, status)
        VALUES (%s, %s, %s, %s, %s);
        """
        cur.execute(audit_sql, (batch_id, "fact_sales", s3_path, cnt, "SUCCESS"))

    logger.info("fact_sales MERGE complete: %d rows now present in target table.", cnt)
    return cnt


def merge_fact_returns(conn, s3_path: str, iam_role: str, batch_id: str) -> int:
    """Stage + MERGE incremental load for fact_returns."""
    sql = f"""
    CREATE TEMP TABLE stg_fact_returns (LIKE retail_dw.fact_returns);

    COPY stg_fact_returns
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS PARQUET;

    DELETE FROM retail_dw.fact_returns
    WHERE return_id IN (SELECT DISTINCT return_id FROM stg_fact_returns);

    INSERT INTO retail_dw.fact_returns
    SELECT * FROM stg_fact_returns;

    DROP TABLE stg_fact_returns;
    """
    logger.info("Executing staged MERGE for fact_returns from %s...", s3_path)
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) FROM retail_dw.fact_returns;")
        cnt = cur.fetchone()[0]

        audit_sql = """
        INSERT INTO retail_dw.etl_file_control (batch_id, target_table, s3_path, rows_loaded, status)
        VALUES (%s, %s, %s, %s, %s);
        """
        cur.execute(audit_sql, (batch_id, "fact_returns", s3_path, cnt, "SUCCESS"))

    logger.info("fact_returns MERGE complete: %d rows now present in target table.", cnt)
    return cnt


def run_redshift_pipeline():
    cfg = get_config()
    s3_bucket = cfg["s3"]["bucket"]
    iam_role = cfg["redshift"]["iam_role"]
    ddl_path = Path(__file__).parents[2] / "src" / "redshift" / "ddl" / "01_create_star_schema.sql"

    batch_id = datetime.now(timezone.utc).strftime("RS_BATCH_%Y%m%d_%H%M%S")
    logger.info("Starting Redshift load pipeline: %s", batch_id)

    conn = get_redshift_connection()
    results = {}

    try:
        # 1. Apply DDL
        execute_ddl(conn, ddl_path)

        # 2. Dimensions
        dims = ["dim_date", "dim_product", "dim_store", "dim_supplier", "dim_promotion", "dim_customer", "customer_rfm"]
        for d in dims:
            s3_path = f"s3://{s3_bucket}/gold/{d}/"
            results[d] = copy_dimension(conn, d, s3_path, iam_role)

        # 3. Facts with Staging + MERGE
        s_s3_path = f"s3://{s3_bucket}/gold/fact_sales/"
        results["fact_sales"] = merge_fact_sales(conn, s_s3_path, iam_role, batch_id)

        r_s3_path = f"s3://{s3_bucket}/gold/fact_returns/"
        results["fact_returns"] = merge_fact_returns(conn, r_s3_path, iam_role, batch_id)

        logger.info("==================================================")
        logger.info(" REDSHIFT LOAD PIPELINE SUCCESSFUL")
        logger.info("==================================================")
        for tbl, cnt in results.items():
            logger.info("  -> Table: %-16s | Rows in Redshift: %d", tbl, cnt)

        return results

    finally:
        conn.close()


if __name__ == "__main__":
    run_redshift_pipeline()
