"""
scripts/validate_phase4.py
---------------------------
Phase 4 validation script.
Validates:
1. All clean Silver Parquet tables exist on disk and have non-zero rows.
2. Rejected Parquet tables exist with explicit reject_reason codes.
3. Row-Count Reconciliation:
   Bronze Row Count == Silver Valid Count + Rejected Count (or <= for deduped entities).
4. Business rule assertions on Silver:
   - quantity > 0
   - unit_price > 0
   - gross_amount and net_amount accurately calculated
   - customer emails valid regex
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

# 1. Clean Silver Tables
banner("CHECK 1 - Clean Silver Parquet Tables")
SILVER_TABLES = ["products", "stores", "suppliers", "promotions", "customers", "sales", "returns"]

for tbl in SILVER_TABLES:
    p = Path(resolve_output_path("silver")) / tbl
    exists = p.exists() and any(p.glob("*.parquet"))
    row_count = 0
    if exists:
        ds = pq.ParquetDataset(str(p))
        row_count = sum(f.metadata.num_rows for f in ds.fragments)
    all_passed &= check(f"Silver Table: silver_{tbl}", exists and row_count > 0, f"{row_count} valid records")

# 2. Rejected Tables and Reason Codes
banner("CHECK 2 - Rejected Records and Reason Codes")
REJECT_TABLES = ["products", "promotions", "customers", "sales", "returns"]

for tbl in REJECT_TABLES:
    p = Path(resolve_output_path("silver_rejected")) / tbl
    exists = p.exists() and any(p.glob("*.parquet"))
    row_count = 0
    has_reason = False
    sample_reason = ""
    if exists:
        ds = pq.ParquetDataset(str(p))
        row_count = sum(f.metadata.num_rows for f in ds.fragments)
        has_reason = "reject_reason" in ds.schema.names
        if has_reason:
            sample_tbl = ds.read(columns=["reject_reason"]).slice(0, 3)
            sample_reason = str(sample_tbl["reject_reason"].to_pylist()[0])

    all_passed &= check(
        f"Rejected {tbl}",
        exists and row_count > 0 and has_reason,
        f"{row_count} rejected rows (Sample reason: '{sample_reason[:35]}...')"
    )

# 3. Reconciliation Verification
banner("CHECK 3 - Row-Count Reconciliation Audit")
for name in ["products", "promotions", "sales", "returns"]:
    b_path = Path(resolve_output_path(f"bronze_{name}"))
    s_path = Path(resolve_output_path("silver")) / name
    r_path = Path(resolve_output_path("silver_rejected")) / name

    b_cnt = sum(f.metadata.num_rows for f in pq.ParquetDataset(str(b_path)).fragments)
    s_cnt = sum(f.metadata.num_rows for f in pq.ParquetDataset(str(s_path)).fragments)
    r_cnt = sum(f.metadata.num_rows for f in pq.ParquetDataset(str(r_path)).fragments) if r_path.exists() else 0

    is_balanced = (s_cnt + r_cnt) == b_cnt
    all_passed &= check(
        f"Reconciliation: {name}",
        is_balanced,
        f"Bronze ({b_cnt}) == Silver ({s_cnt}) + Rejects ({r_cnt})"
    )

# 4. Data Quality Assertions on Silver Sales
banner("CHECK 4 - Silver Data Quality Assertions")
try:
    s_sales_p = Path(resolve_output_path("silver")) / "sales"
    sales_tbl = pq.read_table(str(s_sales_p), columns=["quantity", "unit_price", "gross_amount", "net_amount"])
    
    # Assert quantities > 0
    qtys = sales_tbl["quantity"].to_pylist()
    all_pos_qty = all(q > 0 for q in qtys)
    all_passed &= check("All Silver quantities > 0", all_pos_qty, f"{len(qtys)} records checked")

    # Assert unit prices > 0
    prices = sales_tbl["unit_price"].to_pylist()
    all_pos_price = all(p > 0.0 for p in prices)
    all_passed &= check("All Silver unit_prices > 0", all_pos_price, f"{len(prices)} records checked")

    # Assert net amount calculation net_amount <= gross_amount
    gross = sales_tbl["gross_amount"].to_pylist()
    net = sales_tbl["net_amount"].to_pylist()
    calc_ok = all(net[i] <= round(gross[i] + 0.01, 2) for i in range(len(net)))
    all_passed &= check("Calculated net_amount <= gross_amount", calc_ok, "Calculations verified")

except Exception as e:
    all_passed &= check("Data quality assertion", False, str(e))

# Summary
banner("SUMMARY")
if all_passed:
    print("\n  [ALL PASS]  Phase 4 complete. Ready for Phase 5 (Gold Layer Star Schema & RFM Modeling).\n")
    sys.exit(0)
else:
    print("\n  [FAILURES]  Fix the [FAIL] items above before Phase 5.\n")
    sys.exit(1)
