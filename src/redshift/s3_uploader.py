"""
src/redshift/s3_uploader.py
--------------------------
Uploads local Gold Parquet directories to Amazon S3 for Redshift COPY ingestion.
Uses boto3 to recursively upload files under configured bucket and prefix.
"""
from pathlib import Path
from typing import List, Dict

import boto3
from botocore.exceptions import ClientError

from src.utils.config_loader import get_config, resolve_output_path
from src.utils.logger import get_logger

logger = get_logger("redshift.s3_uploader")


def upload_directory_to_s3(local_dir: Path, s3_bucket: str, s3_prefix: str) -> int:
    """Recursively upload all files in local_dir to s3://s3_bucket/s3_prefix."""
    s3_client = boto3.client("s3")
    uploaded_count = 0

    files = [f for f in local_dir.rglob("*") if f.is_file() and not f.name.startswith(".")]
    logger.info("Uploading %d files from %s to s3://%s/%s...", len(files), local_dir, s3_bucket, s3_prefix)

    for f in files:
        rel_path = f.relative_to(local_dir).as_posix()
        s3_key = f"{s3_prefix}/{rel_path}".replace("//", "/")
        try:
            s3_client.upload_file(str(f), s3_bucket, s3_key)
            uploaded_count += 1
        except ClientError as e:
            logger.error("Failed to upload %s to s3://%s/%s: %s", f, s3_bucket, s3_key, e)
            raise

    logger.info("Successfully uploaded %d files to s3://%s/%s.", uploaded_count, s3_bucket, s3_prefix)
    return uploaded_count


def upload_gold_to_s3() -> Dict[str, int]:
    cfg = get_config()
    s3_bucket = cfg["s3"]["bucket"]
    gold_dir = Path(resolve_output_path("gold"))
    tables = [
        "dim_date", "dim_product", "dim_store", "dim_supplier",
        "dim_promotion", "dim_customer", "fact_sales", "fact_returns", "customer_rfm"
    ]
    results = {}

    for tbl in tables:
        tbl_path = gold_dir / tbl
        if tbl_path.exists():
            s3_prefix = f"gold/{tbl}"
            results[tbl] = upload_directory_to_s3(tbl_path, s3_bucket, s3_prefix)

    return results


if __name__ == "__main__":
    upload_gold_to_s3()
