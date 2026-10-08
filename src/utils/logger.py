"""
src/utils/logger.py
--------------------
Centralized logging setup.
Call setup_logging() once at the entry point of each script.
Use get_logger(name) everywhere else.
"""
import logging
import logging.config
import logging.handlers
from pathlib import Path
from typing import Optional

import yaml


_PROJECT_ROOT = Path(__file__).parents[2]
_INITIALIZED = False


def setup_logging(
    config_path: Optional[str | Path] = None,
    log_dir: Optional[str | Path] = None,
) -> logging.Logger:
    """
    Initialize logging from config/logging.yaml.
    Creates the log directory if it doesn't exist.
    Calling this multiple times is safe (idempotent after first call).

    Returns the root 'retail_pipeline' logger.
    """
    global _INITIALIZED
    if _INITIALIZED:
        return logging.getLogger("retail_pipeline")

    if config_path is None:
        config_path = _PROJECT_ROOT / "config" / "logging.yaml"

    if log_dir is None:
        log_dir = _PROJECT_ROOT / "logs"

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    with open(config_path, encoding="utf-8") as fh:
        log_cfg: dict = yaml.safe_load(fh)

    # Rewrite relative filename to absolute path under log_dir
    for handler_cfg in log_cfg.get("handlers", {}).values():
        if "filename" in handler_cfg:
            handler_cfg["filename"] = str(
                Path(log_dir) / Path(handler_cfg["filename"]).name
            )

    logging.config.dictConfig(log_cfg)
    _INITIALIZED = True

    logger = logging.getLogger("retail_pipeline")
    logger.debug("Logging initialized. Log dir: %s", log_dir)
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Return a named child logger under the 'retail_pipeline' namespace.
    Examples:
        get_logger("bronze.sales")   -> retail_pipeline.bronze.sales
        get_logger("silver.orders")  -> retail_pipeline.silver.orders
    """
    if not _INITIALIZED:
        setup_logging()
    return logging.getLogger(f"retail_pipeline.{name}")
