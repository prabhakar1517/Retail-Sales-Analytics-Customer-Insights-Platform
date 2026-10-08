"""
src/utils/config_loader.py
--------------------------
Loads config/config.yaml and resolves ${ENV_VAR} placeholders from .env.
Call load_config() once at startup; use get_config() / get_path() everywhere else.
"""
import os
import re
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Load .env before anything so ${VAR} placeholders resolve correctly
_PROJECT_ROOT = Path(__file__).parents[2]
load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", override=True)

_CONFIG: dict = {}
_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve(value: Any) -> Any:
    """Recursively replace ${VAR_NAME} placeholders with os.environ values."""
    if isinstance(value, str):
        def _sub(m: re.Match) -> str:
            env_val = os.environ.get(m.group(1), "")
            return env_val
        return _ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(item) for item in value]
    return value


def load_config(config_path: str | Path | None = None) -> dict:
    """
    Load and cache the project config.
    Thread-safe for single-process use (standard for PySpark driver).
    """
    global _CONFIG
    if _CONFIG:
        return _CONFIG

    if config_path is None:
        config_path = _PROJECT_ROOT / "config" / "config.yaml"

    with open(config_path, encoding="utf-8") as fh:
        raw: dict = yaml.safe_load(fh)

    _CONFIG = _resolve(raw)

    # Apply .env overrides for storage mode
    env_mode = os.environ.get("ENVIRONMENT", "").strip()
    if env_mode:
        _CONFIG["environment"] = env_mode

    return _CONFIG


def get_config() -> dict:
    """Return the loaded config dict (loads lazily on first call)."""
    if not _CONFIG:
        load_config()
    return _CONFIG


def get_path(key: str) -> Path:
    """
    Return an absolute Path for the given key in config.paths.
    Example: get_path("bronze") -> /project/root/data/bronze
    """
    cfg = get_config()
    rel: str = cfg["paths"][key]
    return (_PROJECT_ROOT / rel).resolve()


def get_storage_mode() -> str:
    """Return 'local' or 's3'."""
    return get_config().get("storage", {}).get("mode", "local")


def resolve_output_path(logical_name: str) -> str:
    """
    Return the correct write path for a dataset.
    In local mode:  absolute local path string (no scheme)
    In s3 mode:     s3a://bucket/prefix/...
    """
    cfg = get_config()
    mode = get_storage_mode()

    if mode == "local":
        return str(get_path(logical_name))

    # s3 mode
    bucket = cfg["s3"]["bucket"]
    prefix = cfg["s3"]["prefix"].get(logical_name, logical_name)
    return f"s3a://{bucket}/{prefix}"
