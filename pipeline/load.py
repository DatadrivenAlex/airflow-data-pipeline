"""Idempotent raw load. Replays replace event ids; API windows also replace a time partition."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipeline.config import Settings
from pipeline.extract import ExtractResult
from pipeline.records import SourceEvent, collapse_latest, naive_utc, parse_timestamp, read_events
from pipeline.warehouse import RAW_COLUMNS, Warehouse, ensure_storage, open_warehouse


@dataclass(frozen=True)
class LoadMeta:
    load_id: str
    record_source: str
    extracted_at: datetime


@dataclass(frozen=True)
class LoadResult:
    load_id: str
    source: str
    extract_path: str
    row_count: int
    rows_in_file: int
    rows_landed: int
    raw_rows: int
    window_start: datetime
    window_end: datetime
    overwrite_window: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "load_id": self.load_id,
            "source": self.source,
            "extract_path": self.extract_path,
            "row_count": self.row_count,
            "rows_in_file": self.rows_in_file,
            "rows_landed": self.rows_landed,
            "raw_rows": self.raw_rows,
            "window_start": naive_utc(self.window_start).isoformat(timespec="seconds"),
            "window_end": naive_utc(self.window_end).isoformat(timespec="seconds"),
            "overwrite_window": self.overwrite_window,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> LoadResult:
        window_start = parse_timestamp(str(payload["window_start"]))
        window_end = parse_timestamp(str(payload["window_end"]))
        if window_start is None or window_end is None:
            raise ValueError("load payload is missing a window timestamp")
        return cls(
            load_id=str(payload["load_id"]),
            source=str(payload["source"]),
            extract_path=str(payload["extract_path"]),
            row_count=int(str(payload["row_count"])),
            rows_in_file=int(str(payload["rows_in_file"])),
            rows_landed=int(str(payload["rows_landed"])),
            raw_rows=int(str(payload["raw_rows"])),
            window_start=window_start,
            window_end=window_end,
            overwrite_window=_as_bool(payload["overwrite_window"]),
        )


def load_extract(
    settings: Settings,
    extracted: ExtractResult,
    *,
    now: datetime | None = None,
) -> LoadResult:
    events = read_events(Path(extracted.extract_path))
    landed = collapse_latest(events)
    extracted_at = naive_utc(now or datetime.now(UTC))
    meta = LoadMeta(
        load_id=uuid.uuid4().hex,
        record_source=extracted.source,
        extracted_at=extracted_at,
    )
    warehouse = open_warehouse(settings)
    try:
        warehouse.begin()
        ensure_storage(warehouse)
        # Blank ids are not a stable key, so clear them on every load. Otherwise
        # an offline replay of a bad file would append a new copy each run.
        warehouse.execute("DELETE FROM raw.earthquakes WHERE event_id IS NULL")
        _delete_ids(warehouse, [event.event_id for event in landed if event.event_id])
        if extracted.overwrite_window:
            # After id deletes, drop anything still inside the partition that the
            # new file no longer contains (deleted or reclassified USGS events).
            warehouse.execute(
                "DELETE FROM raw.earthquakes "
                f"WHERE event_time >= {warehouse.placeholder} "
                f"AND event_time < {warehouse.placeholder}",
                [extracted.window_start, extracted.window_end],
            )
        insert_events(warehouse, landed, meta)
        _insert_audit(warehouse, extracted, meta, rows_in_file=len(events), rows_landed=len(landed))
        warehouse.commit()
        raw_rows = _as_int(warehouse.fetchone("SELECT COUNT(*) FROM raw.earthquakes"))
    except Exception:
        warehouse.rollback()
        raise
    finally:
        warehouse.close()
    return LoadResult(
        load_id=meta.load_id,
        source=extracted.source,
        extract_path=extracted.extract_path,
        row_count=extracted.row_count,
        rows_in_file=len(events),
        rows_landed=len(landed),
        raw_rows=raw_rows,
        window_start=extracted.window_start,
        window_end=extracted.window_end,
        overwrite_window=extracted.overwrite_window,
    )


def insert_events(warehouse: Warehouse, events: Sequence[SourceEvent], meta: LoadMeta) -> None:
    if not events:
        return
    placeholders = ", ".join([warehouse.placeholder] * len(RAW_COLUMNS))
    columns = ", ".join(RAW_COLUMNS)
    sql = f"INSERT INTO raw.earthquakes ({columns}) VALUES ({placeholders})"
    warehouse.executemany(sql, [_raw_tuple(event, meta) for event in events])


def _insert_audit(
    warehouse: Warehouse,
    extracted: ExtractResult,
    meta: LoadMeta,
    *,
    rows_in_file: int,
    rows_landed: int,
) -> None:
    placeholders = ", ".join([warehouse.placeholder] * 9)
    warehouse.execute(
        "INSERT INTO meta.load_audit ("
        "load_id, record_source, extract_path, window_start, window_end, "
        "rows_in_file, rows_landed, loaded_at, status"
        f") VALUES ({placeholders})",
        [
            meta.load_id,
            meta.record_source,
            extracted.extract_path,
            extracted.window_start,
            extracted.window_end,
            rows_in_file,
            rows_landed,
            meta.extracted_at,
            "loaded",
        ],
    )


def _delete_ids(warehouse: Warehouse, event_ids: list[str]) -> None:
    for chunk in _chunks(event_ids, 500):
        marks = ", ".join([warehouse.placeholder] * len(chunk))
        warehouse.execute(
            f"DELETE FROM raw.earthquakes WHERE event_id IN ({marks})",
            list(chunk),
        )


def _raw_tuple(event: SourceEvent, meta: LoadMeta) -> tuple[object, ...]:
    return (
        event.time_text,
        event.latitude_text,
        event.longitude_text,
        event.depth_text,
        event.mag_text,
        event.mag_type_text,
        event.nst_text,
        event.gap_text,
        event.dmin_text,
        event.rms_text,
        event.net_text,
        event.event_id,
        event.updated_text,
        event.place_text,
        event.event_type_text,
        event.horizontal_error_text,
        event.depth_error_text,
        event.mag_error_text,
        event.mag_nst_text,
        event.status_text,
        event.location_source_text,
        event.mag_source_text,
        event.event_time,
        event.updated_at,
        meta.record_source,
        meta.extracted_at,
        meta.load_id,
    )


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _as_int(row: tuple | None) -> int:
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
