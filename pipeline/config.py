"""Runtime settings. Warehouse and extract paths come from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from pipeline.errors import PipelineError

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SAMPLE_CSV = REPO_ROOT / "data" / "samples" / "usgs_earthquakes.csv"
DEFAULT_DUCKDB_PATH = REPO_ROOT / "data" / "warehouse" / "earthquakes.duckdb"
DEFAULT_EXTRACT_DIR = REPO_ROOT / "data" / "extracts"

# The committed sample is the USGS M4.5+ catalog for this half-open window.
SAMPLE_WINDOW_START = datetime(2024, 1, 1)
SAMPLE_WINDOW_END = datetime(2024, 2, 1)
DEFAULT_MIN_MAGNITUDE = 4.5

_TRUE = {"1", "true", "yes", "y", "on"}
_FALSE = {"0", "false", "no", "n", "off"}


@dataclass(frozen=True)
class Settings:
    offline: bool
    allow_sample_fallback: bool
    duckdb_path: Path
    warehouse_url: str | None
    sample_csv: Path
    extract_dir: Path
    min_magnitude: float
    request_timeout_seconds: int

    def with_overrides(self, **overrides: object) -> Settings:
        return replace(self, **overrides)


def load_settings() -> Settings:
    """Read settings from the environment.

    `ELT_OFFLINE` defaults to off so a scheduled run tries the USGS feed.
    The Docker Compose file turns it on so a laptop demo does not need network.
    """

    return Settings(
        offline=_flag("ELT_OFFLINE", default=False),
        allow_sample_fallback=_flag("ELT_ALLOW_SAMPLE_FALLBACK", default=True),
        duckdb_path=Path(os.getenv("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH))),
        warehouse_url=_blank_to_none(os.getenv("WAREHOUSE_URL")),
        sample_csv=Path(os.getenv("SAMPLE_CSV_PATH", str(DEFAULT_SAMPLE_CSV))),
        extract_dir=Path(os.getenv("EXTRACT_DIR", str(DEFAULT_EXTRACT_DIR))),
        min_magnitude=float(os.getenv("USGS_MIN_MAGNITUDE", str(DEFAULT_MIN_MAGNITUDE))),
        request_timeout_seconds=int(os.getenv("USGS_TIMEOUT_SECONDS", "60")),
    )


def _flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise PipelineError(f"{name} must be a boolean flag, got {raw!r}")


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
