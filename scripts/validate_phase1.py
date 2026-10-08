"""
scripts/validate_phase1.py
---------------------------
Phase 1 validation script.
Runs a series of checks to confirm the environment is correctly set up.

Usage:
    python scripts/validate_phase1.py

Expected: All checks print [PASS]. Fix any [FAIL] before Phase 2.
"""
import io
import sys
import os
from pathlib import Path

# Force UTF-8 output on Windows to avoid cp1252 UnicodeEncodeError
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


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


# CHECK 1
banner("CHECK 1 - Python version")
major, minor = sys.version_info[:2]
v_ok = major == 3 and minor >= 10
all_passed &= check(
    f"Python {major}.{minor} (need 3.10+)",
    v_ok,
    sys.version,
)

# CHECK 2
banner("CHECK 2 - Java version")
import subprocess
try:
    result = subprocess.run(
        ["java", "-version"], capture_output=True, text=True, timeout=10
    )
    java_out = result.stderr or result.stdout
    java_ok = "version" in java_out
    all_passed &= check("Java installed", java_ok, java_out.splitlines()[0] if java_out else "")
except FileNotFoundError:
    all_passed &= check("Java installed", False, "java command not found - install JDK 11+")

# CHECK 3
banner("CHECK 3 - .env file")
env_file = PROJECT_ROOT / ".env"
env_exists = env_file.exists()
all_passed &= check(".env file exists", env_exists,
                    f"{env_file}" if env_exists else "Copy .env.example -> .env and fill in values")

# CHECK 4
banner("CHECK 4 - Config loading")
try:
    from src.utils.config_loader import load_config, get_path
    cfg = load_config()
    cfg_ok = isinstance(cfg, dict) and "storage" in cfg
    all_passed &= check("config.yaml loads", cfg_ok)
    all_passed &= check(
        "storage.mode is set",
        bool(cfg.get("storage", {}).get("mode")),
        cfg.get("storage", {}).get("mode", "MISSING"),
    )
except Exception as exc:
    all_passed &= check("config.yaml loads", False, str(exc))

# CHECK 5
banner("CHECK 5 - Logging setup")
try:
    from src.utils.logger import setup_logging, get_logger
    setup_logging()
    log = get_logger("validate")
    log.info("Logging works")
    log_file = PROJECT_ROOT / "logs" / "pipeline.log"
    all_passed &= check("Logging initializes", True)
    all_passed &= check("Log file created", log_file.exists(), str(log_file))
except Exception as exc:
    all_passed &= check("Logging initializes", False, str(exc))

# CHECK 6
banner("CHECK 6 - Python package imports")
PACKAGES = {
    "pyspark": "pyspark",
    "faker": "faker",
    "boto3": "boto3",
    "psycopg2": "psycopg2",
    "mysql.connector": "mysql-connector-python",
    "oracledb": "oracledb",
    "yaml": "pyyaml",
    "dotenv": "python-dotenv",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "pytest": "pytest",
}
for module, pkg in PACKAGES.items():
    try:
        __import__(module)
        all_passed &= check(f"import {module}", True)
    except ImportError as exc:
        all_passed &= check(
            f"import {module}", False,
            f"pip install {pkg}"
        )

# CHECK 7
banner("CHECK 7 - Raw data file")
raw_csv = PROJECT_ROOT / "data" / "raw" / "online_retail_II.csv"
raw_exists = raw_csv.exists()
all_passed &= check(
    "data/raw/online_retail_II.csv exists",
    raw_exists,
    f"{raw_csv.stat().st_size / 1024 / 1024:.1f} MB" if raw_exists else "Place the file here",
)

if raw_exists:
    try:
        import csv
        with open(raw_csv, encoding="utf-8-sig", errors="replace") as f:
            header = next(csv.reader(f))
        expected = {"Invoice", "StockCode", "Description", "Quantity",
                    "InvoiceDate", "Price", "Customer ID", "Country"}
        header_ok = set(header) == expected
        all_passed &= check(
            "CSV header correct",
            header_ok,
            f"Got: {header}" if not header_ok else f"OK: {header}",
        )
    except Exception as exc:
        all_passed &= check("CSV header readable", False, str(exc))

# CHECK 8
banner("CHECK 8 - JDBC JARs")
EXPECTED_JARS = [
    "ojdbc11-23.6.0.24.10.jar",
    "mysql-connector-j-9.1.0.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
]
jars_dir = PROJECT_ROOT / "jars"
for jar_name in EXPECTED_JARS:
    jar_path = jars_dir / jar_name
    exists = jar_path.exists()
    size_str = f"{jar_path.stat().st_size / 1024 / 1024:.1f} MB" if exists else ""
    all_passed &= check(
        jar_name,
        exists,
        size_str if exists else "Run: python scripts/download_jars.py",
    )

# CHECK 9
banner("CHECK 9 - SparkSession (local mode)")
try:
    from src.utils.spark_session import create_spark_session, stop_spark
    spark = create_spark_session(
        app_name="Phase1Validation",
        master="local[2]",
    )
    version = spark.version
    test_df = spark.range(10)
    count = test_df.count()
    spark_ok = count == 10 and version.startswith("3.")
    all_passed &= check(f"SparkSession v{version} - range(10).count()=={count}", spark_ok)
    stop_spark(spark)
except Exception as exc:
    all_passed &= check("SparkSession creates", False, str(exc))

# CHECK 10
banner("CHECK 10 - File tracker (SQLite)")
try:
    from src.utils.file_tracker import mark_processed, is_processed, list_all
    mark_processed(
        file_path=str(raw_csv),
        source_type="test",
        batch_id="validate_phase1",
        row_count=0,
        status="SUCCESS",
    )
    already = is_processed(str(raw_csv))
    all_passed &= check("File tracker SQLite write+read", already)

    db_path = PROJECT_ROOT / "data" / ".file_tracker.db"
    all_passed &= check("Tracker DB file created", db_path.exists(), str(db_path))
except Exception as exc:
    all_passed &= check("File tracker", False, str(exc))

# SUMMARY
banner("SUMMARY")
if all_passed:
    print("\n  [ALL PASS]  Phase 1 complete. Ready for Phase 2.\n")
    sys.exit(0)
else:
    print("\n  [FAILURES]  Fix the [FAIL] items above before Phase 2.\n")
    sys.exit(1)
