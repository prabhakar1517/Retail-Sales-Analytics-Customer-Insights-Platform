"""
Retail Sales Analytics & Customer Insights Platform
===================================================
Master End-to-End Pipeline Runner
Provides a centralized CLI to orchestrate each phase or run the full pipeline.
"""
import sys
import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

def run_step(description: str, cmd: list) -> bool:
    print("\n" + "=" * 70)
    print(f"  EXECUTING: {description}")
    print("=" * 70)
    res = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if res.returncode != 0:
        print(f"\n[ERROR] Step '{description}' failed with exit code {res.returncode}")
        return False
    print(f"\n[SUCCESS] Step '{description}' completed.")
    return True

def main():
    parser = argparse.ArgumentParser(description="Retail Sales Analytics & Insights Platform Runner")
    parser.add_argument("--all", action="store_true", help="Execute complete pipeline end-to-end")
    parser.add_argument("--generate", action="store_true", help="Run synthetic data generation")
    parser.add_argument("--bronze", action="store_true", help="Execute Bronze PySpark ingestion")
    parser.add_argument("--silver", action="store_true", help="Execute Silver validation and quality gate")
    parser.add_argument("--gold", action="store_true", help="Execute Gold Star Schema and RFM analytics")
    parser.add_argument("--test", action="store_true", help="Execute pytest unit test suite")
    parser.add_argument("--validate", action="store_true", help="Run master validation suite (Phases 1-9)")
    
    args = parser.parse_args()

    # Default to --validate if no arguments provided
    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(0)

    if args.generate or args.all:
        if not run_step("MySQL Catalogs Generation", [sys.executable, "-m", "src.generation.generate_mysql_masters"]):
            sys.exit(1)
        if not run_step("Oracle CRM Master Generation", [sys.executable, "-m", "src.generation.generate_oracle_crm"]):
            sys.exit(1)
        if not run_step("Daily Sales & Returns CSV Generation", [sys.executable, "-m", "src.generation.generate_daily_csv"]):
            sys.exit(1)

    if args.bronze or args.all:
        if not run_step("Bronze PySpark Ingestion Layer", [sys.executable, "-m", "src.bronze.bronze_pipeline"]):
            sys.exit(1)

    if args.silver or args.all:
        if not run_step("Silver Cleansing & Rejection Layer", [sys.executable, "-m", "src.silver.silver_pipeline"]):
            sys.exit(1)

    if args.gold or args.all:
        if not run_step("Gold Star Schema & RFM Mart", [sys.executable, "-m", "src.gold.gold_pipeline"]):
            sys.exit(1)

    if args.test or args.all:
        if not run_step("Pytest Transformation Test Suite", [sys.executable, "-m", "pytest", "tests/", "-v"]):
            sys.exit(1)

    if args.validate:
        if not run_step("Master Phase 1-9 Validation Suite", [sys.executable, "scripts/run_all_validations.py"]):
            sys.exit(1)

    print("\n" + "=" * 70)
    print("  ALL REQUESTED PIPELINE OPERATIONS COMPLETED SUCCESSFULLY!")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
