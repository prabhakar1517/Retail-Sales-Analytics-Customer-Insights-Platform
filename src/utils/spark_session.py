"""
src/utils/spark_session.py
--------------------------
SparkSession factory.
- Local mode (default): master=local[*], no Docker needed.
- Cluster mode: master=spark://spark-master:7077 (Docker Compose).
- S3 mode (Phase 6): adds hadoop-aws config for s3a:// paths.

All JAR files in jars/ are automatically added to spark.jars.
Java 21 compatibility options are always set.
"""
import os
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

from .config_loader import get_config, get_storage_mode
from .logger import get_logger

logger = get_logger("spark_session")
_PROJECT_ROOT = Path(__file__).parents[2]


def _collect_jar_paths() -> str:
    """Return comma-separated absolute paths to all *.jar files in jars/."""
    jars_dir = _PROJECT_ROOT / "jars"
    jars = sorted(jars_dir.glob("*.jar"))
    if not jars:
        logger.warning(
            "No JARs found in %s. Run: python scripts/download_jars.py", jars_dir
        )
        return ""
    paths = ",".join(str(j) for j in jars)
    logger.debug("JARs loaded: %s", paths)
    return paths


def create_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
) -> SparkSession:
    """
    Build and return a configured SparkSession.

    Parameters
    ----------
    app_name : override config spark.app_name (useful in tests)
    master   : override config spark.master  (e.g. 'local[2]' in tests)

    Returns
    -------
    SparkSession (get or create — safe to call multiple times per process)
    """
    # Configure HADOOP_HOME for Windows native filesystem support
    hadoop_dir = _PROJECT_ROOT / "hadoop"
    if hadoop_dir.exists() and "HADOOP_HOME" not in os.environ:
        os.environ["HADOOP_HOME"] = str(hadoop_dir)
        bin_dir = str(hadoop_dir / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
        logger.debug("Set HADOOP_HOME=%s", hadoop_dir)

    # Force Python executable path for PySpark worker processes
    import sys
    py_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = py_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = py_exec

    cfg = get_config()
    spark_cfg = cfg.get("spark", {})
    storage_mode = get_storage_mode()

    resolved_master = master or spark_cfg.get("master", "local[*]")
    resolved_app = app_name or spark_cfg.get("app_name", "RetailAnalyticsPipeline")
    driver_memory = spark_cfg.get("driver_memory", "4g")
    executor_memory = spark_cfg.get("executor_memory", "3g")
    shuffle_partitions = str(spark_cfg.get("shuffle_partitions", 8))
    adaptive = str(spark_cfg.get("adaptive_enabled", True)).lower()
    log_level = spark_cfg.get("log_level", "WARN")

    jar_paths = _collect_jar_paths()

    # ── Java 21 compatibility & Windows memory safety ─────────────────────────
    java21_opts = (
        "-XX:+TieredCompilation -XX:TieredStopAtLevel=1 "
        "-XX:CompressedClassSpaceSize=64m -XX:MaxMetaspaceSize=192m "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED"
    )

    builder = (
        SparkSession.builder
        .appName(resolved_app)
        .master(resolved_master)
        # Memory
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        # Performance
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", adaptive)
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Timezone — always UTC to avoid silent offset bugs
        .config("spark.sql.session.timeZone", "UTC")
        # Parquet rebase mode (avoids warnings on dates before 1582-10-15)
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        # Java 21 module access
        .config("spark.driver.extraJavaOptions", java21_opts)
        .config("spark.executor.extraJavaOptions", java21_opts)
        # Avoid OOM on wide DataFrames (100 MB threshold)
        .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
    )

    if jar_paths:
        builder = builder.config("spark.jars", jar_paths)

    # ── S3 / hadoop-aws configuration (Phase 6+) ──────────────────────────────
    if storage_mode == "s3":
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        aws_region = os.environ.get("AWS_REGION", "ap-south-1")

        if not aws_key or not aws_secret:
            raise EnvironmentError(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set "
                "when storage.mode = 's3'. Check your .env file."
            )

        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", aws_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
            .config("spark.hadoop.fs.s3a.endpoint",
                    f"s3.{aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100 MB
        )
        logger.info("S3 storage mode configured (region=%s, bucket=%s)",
                    aws_region, cfg["s3"]["bucket"])

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    logger.info(
        "SparkSession ready  app=%s  master=%s  storage=%s  "
        "shuffle_partitions=%s  driver_mem=%s",
        resolved_app, resolved_master, storage_mode, shuffle_partitions, driver_memory
    )
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    try:
        spark.stop()
        logger.info("SparkSession stopped.")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error stopping SparkSession: %s", exc)
