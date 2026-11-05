"""Shared fixtures. Airflow settings are applied before any Airflow import."""

from __future__ import annotations

import csv
import os
from datetime import datetime
from pathlib import Path

import pytest

from pipeline.config import DEFAULT_SAMPLE_CSV, Settings
from pipeline.records import CSV_COLUMNS
from pipeline.warehouse import Warehouse, open_warehouse

_ROOT = Path(__file__).resolve().parents[1]
_AIRFLOW_HOME = _ROOT / ".airflow"
_AIRFLOW_HOME.mkdir(exist_ok=True)
_AIRFLOW_DB = (_AIRFLOW_HOME / "airflow.db").resolve()

os.environ["AIRFLOW_HOME"] = str(_AIRFLOW_HOME)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(_ROOT / "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
os.environ["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] = "True"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{_AIRFLOW_DB}"
os.environ["AIRFLOW__CORE__FERNET_KEY"] = "6i8Gdc7EqDn7nPa-dfsr3mJ0OgOyzw00ZyeIpLf3qnw="
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "ERROR"

JAN1 = datetime(2024, 1, 1)
JAN2 = datetime(2024, 1, 2)
JAN3 = datetime(2024, 1, 3)


def make_settings(tmp_path: Path, **overrides: object) -> Settings:
    settings = Settings(
        offline=True,
        allow_sample_fallback=True,
        duckdb_path=tmp_path / "earthquakes.duckdb",
        warehouse_url=None,
        sample_csv=DEFAULT_SAMPLE_CSV,
        extract_dir=tmp_path / "extracts",
        min_magnitude=4.5,
        request_timeout_seconds=5,
    )
    if overrides:
        return settings.with_overrides(**overrides)
    return settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


def write_feed(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            payload = {column: "" for column in CSV_COLUMNS}
            payload.update(row)
            writer.writerow(payload)


def feed_row(
    event_id: str,
    time: str,
    mag: str,
    place: str,
    *,
    event_type: str = "earthquake",
    updated: str | None = None,
    latitude: str = "10",
    longitude: str = "20",
    depth: str = "10",
) -> dict[str, str]:
    return {
        "id": event_id,
        "time": time,
        "mag": mag,
        "place": place,
        "type": event_type,
        "updated": updated or time,
        "latitude": latitude,
        "longitude": longitude,
        "depth": depth,
        "magType": "mb",
        "status": "reviewed",
        "net": "us",
    }


def fetch_all(settings: Settings, sql: str) -> list[tuple]:
    warehouse: Warehouse = open_warehouse(settings)
    try:
        return warehouse.fetchall(sql)
    finally:
        warehouse.close()
