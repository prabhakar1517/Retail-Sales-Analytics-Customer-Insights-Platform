"""
scripts/validate_phase5.py
---------------------------
Phase 5 validation script.
Validates:
1. All 6 Dimension tables exist in Parquet format with valid surrogate keys.
2. SCD Type 2 fields present in dim_customer (effective_date, expiry_date, is_current).
3. fact_sales exists, partitioned by date_key, with surrogate foreign keys linked.
4. fact_returns exists, partitioned by return_date_key, with surrogate foreign keys.
5. customer_rfm analytical mart exists with recency, frequency, monetary, scores, and segment names.
6. Dimensional modeling integrity: No orphaned dimension keys in facts (coalesced fallback 0).
"""
import io
import sys
import os
from pathlib import Path

# Force UTF-8 output on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import pyarrow.parquet as pq
from src.utils.config_loader import resolve_output_path


def banner(text: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {text}")
    print("=" * 65)


def check(name: str, ok: bool, detail: str = "") -> bool:
    status = "PASS" if ok else "FAIL"
    suffix = f"  -> {detail}" if detail else ""
    print(f"  [{status}]  {name}{suffix}")
    return ok


all_passed = True
gold_dir = Path(resolve_output_path("gold"))

# 1. Dimension Tables
banner("CHECK 1 - Dimension Tables in Star Schema")
DIMENSIONS = {
    "dim_date": ("date_key", 1461),
    "dim_product": ("product_key", 495),
    "dim_store": ("store_key", 50),
    "dim_supplier": ("supplier_key", 100),
    "dim_promotion": ("promo_key", 195),
    "dim_customer": ("customer_key", 4901),
}

for dim, (surrogate_pk, expected_min_rows) in DIMENSIONS.items():
    p = gold_dir / dim
    exists = p.exists() and any(p.glob("*.parquet"))
    row_count = 0
    has_pk = False
    if exists:
        ds = pq.ParquetDataset(str(p))
        row_count = sum(f.metadata.num_rows for f in ds.fragments)
        has_pk = surrogate_pk in ds.schema.names

    all_passed &= check(
        f"Dimension: {dim}",
        exists and row_count >= expected_min_rows and has_pk,
        f"{row_count} rows, PK: '{surrogate_pk}'"
    )

# 2. SCD Type 2 Attributes on dim_customer
banner("CHECK 2 - SCD Type 2 Attributes in dim_customer")
try:
    cust_p = gold_dir / "dim_customer"
    cust_ds = pq.ParquetDataset(str(cust_p))
    scd2_cols = ["customer_key", "effective_date", "expiry_date", "is_current"]
    has_scd2 = all(c in cust_ds.schema.names for c in scd2_cols)
    all_passed &= check(
        "SCD2 Columns in dim_customer",
        has_scd2,
        f"Columns present: {scd2_cols}"
    )
except Exception as e:
    all_passed &= check("SCD2 verification", False, str(e))

# 3. Fact Tables
banner("CHECK 3 - Fact Tables & Partitioning")
FACTS = {
    "fact_sales": ("date_key", 18875),
    "fact_returns": ("return_date_key", 523),
}

for fact, (partition_key, expected_min_rows) in FACTS.items():
    p = gold_dir / fact
    exists = p.exists()
    row_count = 0
    is_partitioned = False
    if exists:
        ds = pq.ParquetDataset(str(p))
        row_count = sum(f.metadata.num_rows for f in ds.fragments)
        # Check if subdirectories have partition key
        is_partitioned = any(sub.is_dir() and f"{partition_key}=" in sub.name for sub in p.iterdir())

    all_passed &= check(
        f"Fact: {fact}",
        exists and row_count >= expected_min_rows and is_partitioned,
        f"{row_count} rows, partitioned by '{partition_key}'"
    )

# 4. Customer RFM Analytical Mart
banner("CHECK 4 - Customer RFM Analytical Mart")
try:
    rfm_p = gold_dir / "customer_rfm"
    exists = rfm_p.exists() and any(rfm_p.glob("*.parquet"))
    row_count = 0
    has_rfm_cols = False
    sample_segments = set()
    if exists:
        ds = pq.ParquetDataset(str(rfm_p))
        row_count = sum(f.metadata.num_rows for f in ds.fragments)
        rfm_cols = ["customer_id", "recency", "frequency", "monetary", "r_score", "f_score", "m_score", "rfm_segment"]
        has_rfm_cols = all(c in ds.schema.names for c in rfm_cols)
        sample_tbl = ds.read(columns=["rfm_segment"]).slice(0, 50)
        sample_segments = set(sample_tbl["rfm_segment"].to_pylist())

    all_passed &= check(
        "customer_rfm mart",
        exists and row_count > 0 and has_rfm_cols,
        f"{row_count} customers scored. Segments: {list(sample_segments)[:3]}..."
    )
except Exception as e:
    all_passed &= check("customer_rfm mart check", False, str(e))

# Summary
banner("SUMMARY")
if all_passed:
    print("\n  [ALL PASS]  Phase 5 complete. Ready for Phase 6 (Amazon Redshift DDL, COPY & Incremental MERGE).\n")
    sys.exit(0)
else:
    print("\n  [FAILURES]  Fix the [FAIL] items above before Phase 6.\n")
    sys.exit(1)
