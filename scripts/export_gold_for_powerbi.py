"""
scripts/export_gold_for_powerbi.py
----------------------------------
Consolidates all Gold Star Schema partitioned folders into clean, single
flat files in 'data/powerbi_ready/' for 1-click loading into Power BI Desktop.
Generates both .csv and .parquet for maximum convenience.
"""
import sys
from pathlib import Path
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
OUTPUT_DIR = PROJECT_ROOT / "data" / "powerbi_ready"

def export_for_powerbi():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 70)
    print("Consolidating Gold Star Schema tables for Power BI Desktop...")
    print("=" * 70)

    tables = [
        "dim_date", "dim_product", "dim_store", "dim_supplier",
        "dim_promotion", "dim_customer", "fact_sales", "fact_returns",
        "customer_rfm"
    ]

    for tbl in tables:
        src = GOLD_DIR / tbl
        if not src.exists():
            print(f"  [SKIP] {tbl} does not exist in {GOLD_DIR}")
            continue

        print(f"  Exporting {tbl:<16}...", end="", flush=True)
        arrow_table = pq.read_table(str(src))
        df = arrow_table.to_pandas()

        # Save single Parquet file
        parquet_out = OUTPUT_DIR / f"{tbl}.parquet"
        df.to_parquet(parquet_out, index=False)

        # Save single CSV file (standard, easiest for Power BI beginners)
        csv_out = OUTPUT_DIR / f"{tbl}.csv"
        df.to_csv(csv_out, index=False)

        print(f" Done! ({len(df):,} rows -> {tbl}.csv & {tbl}.parquet)")

    print("\n" + "=" * 70)
    print(f"SUCCESS! All Power BI tables ready in:\n{OUTPUT_DIR}")
    print("=" * 70)

if __name__ == "__main__":
    export_for_powerbi()
