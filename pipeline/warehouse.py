"""DuckDB by default, Postgres when WAREHOUSE_URL is set."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

import duckdb

from pipeline.config import Settings
from pipeline.errors import PipelineError

RAW_COLUMNS = [
    "time_text",
    "latitude_text",
    "longitude_text",
    "depth_text",
    "mag_text",
    "mag_type_text",
    "nst_text",
    "gap_text",
    "dmin_text",
    "rms_text",
    "net_text",
    "event_id",
    "updated_text",
    "place_text",
    "event_type_text",
    "horizontal_error_text",
    "depth_error_text",
    "mag_error_text",
    "mag_nst_text",
    "status_text",
    "location_source_text",
    "mag_source_text",
    "event_time",
    "updated_at",
    "record_source",
    "extracted_at",
    "load_id",
]

_RAW_DDL = """
CREATE TABLE IF NOT EXISTS raw.earthquakes (
    time_text VARCHAR,
    latitude_text VARCHAR,
    longitude_text VARCHAR,
    depth_text VARCHAR,
    mag_text VARCHAR,
    mag_type_text VARCHAR,
    nst_text VARCHAR,
    gap_text VARCHAR,
    dmin_text VARCHAR,
    rms_text VARCHAR,
    net_text VARCHAR,
    event_id VARCHAR,
    updated_text VARCHAR,
    place_text VARCHAR,
    event_type_text VARCHAR,
    horizontal_error_text VARCHAR,
    depth_error_text VARCHAR,
    mag_error_text VARCHAR,
    mag_nst_text VARCHAR,
    status_text VARCHAR,
    location_source_text VARCHAR,
    mag_source_text VARCHAR,
    event_time TIMESTAMP,
    updated_at TIMESTAMP,
    record_source VARCHAR NOT NULL,
    extracted_at TIMESTAMP NOT NULL,
    load_id VARCHAR NOT NULL
)
"""

_AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS meta.load_audit (
    load_id VARCHAR PRIMARY KEY,
    record_source VARCHAR NOT NULL,
    extract_path VARCHAR NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    rows_in_file INTEGER NOT NULL,
    rows_landed INTEGER NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    status VARCHAR NOT NULL
)
"""


class Warehouse(Protocol):
    placeholder: str

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...

    def execute(self, sql: str, params: Sequence[object] | None = None) -> None: ...

    def executemany(self, sql: str, params: Sequence[Sequence[object]]) -> None: ...

    def fetchall(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]: ...

    def fetchone(self, sql: str, params: Sequence[object] | None = None) -> tuple | None: ...


class DuckDBWarehouse:
    placeholder = "?"

    def __init__(self, path: Path):
        self._conn = duckdb.connect(str(path))

    def begin(self) -> None:
        self._conn.begin()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def execute(self, sql: str, params: Sequence[object] | None = None) -> None:
        self._conn.execute(sql, list(params) if params else None)

    def executemany(self, sql: str, params: Sequence[Sequence[object]]) -> None:
        self._conn.executemany(sql, [list(row) for row in params])

    def fetchall(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]:
        result = self._conn.execute(sql, list(params) if params else None)
        return [tuple(row) for row in result.fetchall()]

    def fetchone(self, sql: str, params: Sequence[object] | None = None) -> tuple | None:
        result = self._conn.execute(sql, list(params) if params else None)
        row = result.fetchone()
        if row is None:
            return None
        return tuple(row)


class PostgresWarehouse:
    placeholder = "%s"

    def __init__(self, url: str):
        try:
            import psycopg2
        except ImportError as exc:
            raise PipelineError(
                "WAREHOUSE_URL is set but psycopg2 is not installed. "
                "Install the project requirements; Apache Airflow provides psycopg2."
            ) from exc
        self._conn = psycopg2.connect(url, connect_timeout=10)
        self._conn.autocommit = False

    def begin(self) -> None:
        # psycopg2 opens a transaction on the first statement when autocommit is off.
        return

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def execute(self, sql: str, params: Sequence[object] | None = None) -> None:
        cursor = self._run(sql, params)
        cursor.close()

    def executemany(self, sql: str, params: Sequence[Sequence[object]]) -> None:
        cursor = self._conn.cursor()
        try:
            cursor.executemany(sql, [tuple(row) for row in params])
        finally:
            cursor.close()

    def fetchall(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]:
        cursor = self._run(sql, params)
        try:
            return [tuple(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def fetchone(self, sql: str, params: Sequence[object] | None = None) -> tuple | None:
        cursor = self._run(sql, params)
        try:
            row = cursor.fetchone()
        finally:
            cursor.close()
        if row is None:
            return None
        return tuple(row)

    def _run(self, sql: str, params: Sequence[object] | None):
        cursor = self._conn.cursor()
        cursor.execute(sql, tuple(params) if params else None)
        return cursor


def open_warehouse(settings: Settings) -> Warehouse:
    if settings.warehouse_url:
        return PostgresWarehouse(settings.warehouse_url)
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return DuckDBWarehouse(settings.duckdb_path)


def ensure_storage(warehouse: Warehouse) -> None:
    for schema in ("raw", "staging", "marts", "meta"):
        warehouse.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    warehouse.execute(_RAW_DDL)
    warehouse.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_earthquakes_event_id ON raw.earthquakes (event_id)"
    )
    warehouse.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_earthquakes_event_time ON raw.earthquakes (event_time)"
    )
    warehouse.execute(_AUDIT_DDL)


def reset_storage(warehouse: Warehouse) -> None:
    """Drop pipeline schemas. Used by the Postgres integration test between cases."""

    for schema in ("marts", "staging", "raw", "meta"):
        warehouse.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
