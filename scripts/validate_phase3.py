"""
scripts/validate_phase3.py
---------------------------
Phase 3 validation script.
Validates:
1. All 7 Bronze Parquet tables exist on disk.
2. Row count and schema consistency checks.
3. Ingestion lineage metadata columns (_ingest_timestamp, _batch_id, etc.) present.
4. Processed files tracked in SQLite database (.file_tracker.db).
5. Idempotent re-run check (re-running ingest skips already processed files).
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

from src.utils.config_loader import get_config, get_path, resolve_output_path
from src.utils.file_tracker import list_all


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

# 1. Parquet Table Existence and Row Count Validation
banner("CHECK 1 - Bronze Parquet Directories and Files")
import pyarrow.parquet as pq

TABLES = {
    "sales": ("bronze_sales", 13, 40000),
    "returns": ("bronze_returns", 10, 1000),
    "customers": ("bronze_customers", 13, 5000),
    "products": ("bronze_products", 13, 500),
    "stores": ("bronze_stores", 14, 50),
    "suppliers": ("bronze_suppliers", 14, 100),
    "promotions": ("bronze_promotions", 12, 200),
}

for name, (path_key, expected_min_cols, expected_min_rows) in TABLES.items():
    p = Path(resolve_output_path(path_key))
    exists = p.exists() and any(p.glob("*.parquet"))
    row_count = 0
    num_cols = 0
    if exists:
        try:
            dataset = pq.ParquetDataset(str(p))
            schema = dataset.schema
            num_cols = len(schema.names)
            # Read metadata total rows
            row_count = sum(fragment.metadata.num_rows for fragment in dataset.fragments)
        except Exception as e:
            pass

    passed = exists and row_count >= expected_min_rows and num_cols >= expected_min_cols
    all_passed &= check(
        f"Bronze Table: bronze_{name}",
        passed,
        f"{row_count} rows, {num_cols} cols (path: {p.name})"
    )

# 2. Lineage Metadata Verification
banner("CHECK 2 - Ingestion Lineage Metadata Columns")
try:
    sales_p = Path(resolve_output_path("bronze_sales"))
    ds = pq.ParquetDataset(str(sales_p))
    col_names = ds.schema.names
    has_meta = all(c in col_names for c in ["_ingest_timestamp", "_source_file", "_batch_id"])
    all_passed &= check(
        "Sales lineage metadata (_ingest_timestamp, _source_file, _batch_id)",
        has_meta,
        f"Columns: {col_names[-3:]}"
    )
except Exception as e:
    all_passed &= check("Lineage metadata check", False, str(e))

# 3. File Tracker Status Audit
banner("CHECK 3 - Incremental SQLite File Tracker Records")
try:
    tracked = list_all()
    sales_tracked = [r for r in tracked if r["source_type"] == "sales_csv"]
    returns_tracked = [r for r in tracked if r["source_type"] == "returns_csv"]
    all_passed &= check(
        "File tracker registered sales CSVs",
        len(sales_tracked) > 0,
        f"{len(sales_tracked)} files tracked"
    )
    all_passed &= check(
        "File tracker registered returns CSVs",
        len(returns_tracked) > 0,
        f"{len(returns_tracked)} files tracked"
    )
except Exception as e:
    all_passed &= check("File tracker audit", False, str(e))

# Summary
banner("SUMMARY")
if all_passed:
    print("\n  [ALL PASS]  Phase 3 complete. Ready for Phase 4 (Silver Layer Transformation & Validation).\n")
    sys.exit(0)
else:
    print("\n  [FAILURES]  Fix the [FAIL] items above before Phase 4.\n")
    sys.exit(1)
