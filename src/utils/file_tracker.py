"""
src/utils/file_tracker.py
--------------------------
SQLite-backed incremental file tracker.
Records which CSV files have been processed so re-runs skip already-ingested files.
Stored at: data/.file_tracker.db (configured via config.paths.file_tracker_db)
"""
import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .config_loader import get_path
from .logger import get_logger

logger = get_logger("file_tracker")

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_files (
    file_path      TEXT    NOT NULL,
    file_hash      TEXT,
    source_type    TEXT    NOT NULL,
    batch_id       TEXT    NOT NULL,
    row_count      INTEGER NOT NULL DEFAULT 0,
    processed_at   TEXT    NOT NULL,
    status         TEXT    NOT NULL DEFAULT 'SUCCESS',
    error_message  TEXT,
    PRIMARY KEY (file_path)
)
"""


def _db_path() -> str:
    return str(get_path("file_tracker_db"))


def _connect() -> sqlite3.Connection:
    db = _db_path()
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db, timeout=30)
    conn.execute(_CREATE_TABLE_SQL)
    conn.commit()
    return conn


def sha256_of_file(file_path: str, chunk: int = 65536) -> str:
    """SHA-256 hash of file bytes — used to detect re-deliveries of same data."""
    h = hashlib.sha256()
    with open(file_path, "rb") as fh:
        while data := fh.read(chunk):
            h.update(data)
    return h.hexdigest()


def is_processed(file_path: str) -> bool:
    """True if file has a SUCCESS record in the tracker."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM processed_files WHERE file_path = ? AND status = 'SUCCESS'",
            (str(file_path),),
        ).fetchone()
        result = row is not None
        if result:
            logger.debug("Skipping already-processed file: %s", file_path)
        return result
    finally:
        conn.close()


def mark_processed(
    file_path: str,
    source_type: str,
    batch_id: str,
    row_count: int,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
) -> None:
    """
    Upsert a file record.

    Parameters
    ----------
    file_path     : absolute path to the file
    source_type   : 'sales_csv' | 'returns_csv' | 'oracle_crm' | 'mysql_*'
    batch_id      : pipeline run ID (e.g. timestamp string)
    row_count     : number of rows written to Bronze
    status        : 'SUCCESS' | 'FAILED'
    error_message : optional error string for FAILED records
    """
    file_hash = ""
    if os.path.exists(file_path):
        try:
            file_hash = sha256_of_file(file_path)
        except OSError:
            pass

    conn = _connect()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO processed_files
                (file_path, file_hash, source_type, batch_id,
                 row_count, processed_at, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(file_path),
                file_hash,
                source_type,
                batch_id,
                row_count,
                datetime.now(timezone.utc).isoformat(),
                status,
                error_message,
            ),
        )
        conn.commit()
        logger.info(
            "Tracker updated: %-10s  %-8s  rows=%-8d  file=%s",
            status, source_type, row_count, Path(file_path).name,
        )
    finally:
        conn.close()


def get_unprocessed_files(
    directory: str,
    glob_pattern: str = "*.csv",
) -> list[str]:
    """
    Return sorted list of files in *directory* matching *glob_pattern*
    that have NOT been successfully processed yet.
    """
    conn = _connect()
    try:
        files = sorted(Path(directory).glob(glob_pattern))
        result = []
        for fp in files:
            fp_str = str(fp)
            row = conn.execute(
                "SELECT 1 FROM processed_files WHERE file_path = ? AND status = 'SUCCESS'",
                (fp_str,),
            ).fetchone()
            if row is None:
                result.append(fp_str)
        logger.info(
            "Found %d unprocessed file(s) in %s  (pattern=%s)",
            len(result), directory, glob_pattern,
        )
        return result
    finally:
        conn.close()


def list_all(source_type: Optional[str] = None) -> list[dict]:
    """Return all tracker records (optionally filtered by source_type)."""
    conn = _connect()
    try:
        if source_type:
            rows = conn.execute(
                "SELECT * FROM processed_files WHERE source_type = ? ORDER BY processed_at",
                (source_type,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM processed_files ORDER BY processed_at"
            ).fetchall()
        cols = [
            "file_path", "file_hash", "source_type", "batch_id",
            "row_count", "processed_at", "status", "error_message",
        ]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()
