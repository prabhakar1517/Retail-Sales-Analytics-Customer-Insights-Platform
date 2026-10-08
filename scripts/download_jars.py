"""
scripts/download_jars.py
------------------------
Downloads required JDBC and Hadoop/AWS JARs from Maven Central.
Run ONCE before Phase 3 (Bronze ingestion).

Usage:
    python scripts/download_jars.py

Output: jars/ directory at project root with all four JARs.
"""
import sys
import io
import urllib.request
from pathlib import Path

# Force UTF-8 output on Windows to avoid cp1252 UnicodeEncodeError in PowerShell
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


# ─────────────────────────────────────────────────────────────────────────────
# JAR manifest — name -> Maven Central URL
# ─────────────────────────────────────────────────────────────────────────────
JARS = {
    # Oracle JDBC driver (ojdbc11 = JDBC 4.3, supports Java 11/17/21)
    "ojdbc11-23.6.0.24.10.jar": (
        "https://repo1.maven.org/maven2/com/oracle/database/jdbc/"
        "ojdbc11/23.6.0.24.10/ojdbc11-23.6.0.24.10.jar"
    ),
    # MySQL Connector/J 9.x — JDBC 4.2, compatible with MySQL 8.4
    "mysql-connector-j-9.1.0.jar": (
        "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/"
        "9.1.0/mysql-connector-j-9.1.0.jar"
    ),
    # hadoop-aws 3.3.4 — pairs with PySpark/Spark 3.5.x's bundled Hadoop 3.3.4
    "hadoop-aws-3.3.4.jar": (
        "https://repo1.maven.org/maven2/org/apache/hadoop/"
        "hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    ),
    # AWS SDK bundle — must match hadoop-aws expectations exactly
    "aws-java-sdk-bundle-1.12.262.jar": (
        "https://repo1.maven.org/maven2/com/amazonaws/"
        "aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    ),
}

PROJECT_ROOT = Path(__file__).parents[1]
JARS_DIR = PROJECT_ROOT / "jars"


def _progress_hook(block_num: int, block_size: int, total_size: int) -> None:
    downloaded = block_num * block_size
    if total_size > 0:
        pct = min(100, downloaded * 100 // total_size)
        bar = "#" * (pct // 2)
        mb = downloaded / 1024 / 1024
        total_mb = total_size / 1024 / 1024
        sys.stdout.write(
            f"\r  [{bar:<50}] {pct:3d}%  {mb:6.1f} MB / {total_mb:.1f} MB"
        )
        sys.stdout.flush()
    if block_num * block_size >= total_size:
        print()  # newline after completion


def download_jar(name: str, url: str) -> None:
    dest = JARS_DIR / name
    if dest.exists():
        size_mb = dest.stat().st_size / 1024 / 1024
        print(f"  [SKIP] Already present: {name}  ({size_mb:.1f} MB)")
        return

    print(f"\n[DOWN] Downloading: {name}")
    print(f"       Source: {url}")
    try:
        urllib.request.urlretrieve(url, dest, reporthook=_progress_hook)
        size_mb = dest.stat().st_size / 1024 / 1024
        print(f"  [OK]   Saved to: {dest}  ({size_mb:.1f} MB)")
    except Exception as exc:
        print(f"\n  [ERR]  FAILED: {exc}")
        if dest.exists():
            dest.unlink()
        sys.exit(1)



def main() -> None:
    JARS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print(" Retail Analytics Pipeline -- JAR Downloader")
    print(f" Target: {JARS_DIR}")
    print("=" * 65)

    for name, url in JARS.items():
        download_jar(name, url)

    print("\n" + "=" * 65)
    print(" All JARs ready. Summary:")
    print("=" * 65)
    for j in sorted(JARS_DIR.glob("*.jar")):
        size_mb = j.stat().st_size / 1024 / 1024
        print(f"  {j.name:<50}  {size_mb:7.1f} MB")
    print("\nNext step: proceed to Phase 2 (data generation).")


if __name__ == "__main__":
    main()
