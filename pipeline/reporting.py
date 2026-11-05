"""Human-readable summary of one successful ELT run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from pipeline.config import Settings
from pipeline.load import LoadResult
from pipeline.quality import CheckOutcome
from pipeline.warehouse import open_warehouse


@dataclass(frozen=True)
class RunReport:
    source: str
    overwrite_window: bool
    window_start: datetime
    window_end: datetime
    load_id: str
    rows_in_file: int
    rows_landed: int
    raw_rows: int
    staging_rows: int
    daily_rows: int
    region_rows: int
    significant_rows: int
    max_magnitude: object
    audit_status: str
    checks: list[CheckOutcome]
    daily: list[tuple]
    regions: list[tuple]
    significant: list[tuple]


def collect_report(
    settings: Settings,
    loaded: LoadResult,
    checks: list[CheckOutcome],
) -> RunReport:
    warehouse = open_warehouse(settings)
    try:
        daily = warehouse.fetchall(
            "SELECT event_date, event_count, avg_magnitude, max_magnitude, "
            "avg_depth_km, significant_event_count "
            "FROM marts.mart_daily_seismicity ORDER BY event_date"
        )
        regions = warehouse.fetchall(
            "SELECT region, event_count, avg_magnitude, max_magnitude "
            "FROM marts.mart_region_summary "
            "ORDER BY event_count DESC, region "
            "LIMIT 10"
        )
        significant = warehouse.fetchall(
            "SELECT event_time, magnitude, region, place "
            "FROM marts.mart_significant_events "
            "ORDER BY magnitude DESC, event_time "
            "LIMIT 8"
        )
        staging_rows = _int(warehouse.fetchone("SELECT COUNT(*) FROM staging.stg_earthquakes"))
        daily_rows = _int(warehouse.fetchone("SELECT COUNT(*) FROM marts.mart_daily_seismicity"))
        region_rows = _int(warehouse.fetchone("SELECT COUNT(*) FROM marts.mart_region_summary"))
        significant_rows = _int(
            warehouse.fetchone("SELECT COUNT(*) FROM marts.mart_significant_events")
        )
        max_row = warehouse.fetchone("SELECT MAX(magnitude) FROM staging.stg_earthquakes")
        audit = warehouse.fetchone(
            f"SELECT status FROM meta.load_audit WHERE load_id = {warehouse.placeholder}",
            [loaded.load_id],
        )
    finally:
        warehouse.close()
    return RunReport(
        source=loaded.source,
        overwrite_window=loaded.overwrite_window,
        window_start=loaded.window_start,
        window_end=loaded.window_end,
        load_id=loaded.load_id,
        rows_in_file=loaded.rows_in_file,
        rows_landed=loaded.rows_landed,
        raw_rows=loaded.raw_rows,
        staging_rows=staging_rows,
        daily_rows=daily_rows,
        region_rows=region_rows,
        significant_rows=significant_rows,
        max_magnitude=None if max_row is None else max_row[0],
        audit_status="" if audit is None or audit[0] is None else str(audit[0]),
        checks=list(checks),
        daily=daily,
        regions=regions,
        significant=significant,
    )


def format_report(report: RunReport) -> str:
    lines = [
        "USGS earthquake ELT",
        f"run_source: {report.source}",
        f"overwrite_window: {str(report.overwrite_window).lower()}",
        f"window_start: {_format_value(report.window_start)}",
        f"window_end: {_format_value(report.window_end)}",
        f"load_id: {report.load_id}",
        f"rows_in_file: {report.rows_in_file}",
        f"rows_landed: {report.rows_landed}",
        f"raw_rows: {report.raw_rows}",
        f"staging_rows: {report.staging_rows}",
        f"daily_rows: {report.daily_rows}",
        f"region_rows: {report.region_rows}",
        f"significant_rows: {report.significant_rows}",
        f"max_magnitude: {_format_value(report.max_magnitude)}",
        f"audit_status: {report.audit_status}",
        f"quality: pass ({len(report.checks)} checks)",
    ]
    for check in report.checks:
        lines.append(f"- {check.check_id}: {check.detail}")
    lines.extend(
        [
            "",
            "mart_daily_seismicity",
            _table(
                [
                    "event_date",
                    "event_count",
                    "avg_magnitude",
                    "max_magnitude",
                    "avg_depth_km",
                    "significant_event_count",
                ],
                report.daily,
            ),
            "",
            "mart_region_summary (top 10 by event_count)",
            _table(["region", "event_count", "avg_magnitude", "max_magnitude"], report.regions),
            "",
            "mart_significant_events (top 8 by magnitude)",
            _table(["event_time", "magnitude", "region", "place"], report.significant),
            "",
        ]
    )
    return "\n".join(lines)


def _table(headers: list[str], rows: list[tuple]) -> str:
    rendered = [[_format_value(cell) for cell in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in rendered:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def line(cells: list[str]) -> str:
        return " | ".join(cell.ljust(widths[index]) for index, cell in enumerate(cells))

    separator = "-+-".join("-" * width for width in widths)
    body = [line(headers), separator]
    body.extend(line(row) for row in rendered)
    return "\n".join(body)


def _format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.microsecond:
            return value.isoformat(sep=" ", timespec="milliseconds")
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return f"{value:.10f}".rstrip("0").rstrip(".")
    return str(value)


def _int(row: tuple | None) -> int:
    if row is None or row[0] is None:
        return 0
    return int(row[0])
