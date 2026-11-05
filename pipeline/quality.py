"""Data-quality checks declared in transforms/schema.yml. Failures raise DataQualityError."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from pipeline.config import Settings
from pipeline.errors import DataQualityError, PipelineError
from pipeline.records import naive_utc, parse_timestamp
from pipeline.transform import TRANSFORMS_DIR
from pipeline.warehouse import Warehouse, open_warehouse

SCHEMA_PATH = TRANSFORMS_DIR / "schema.yml"

_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
_WHERE = re.compile(r"^[a-z_][a-z0-9_]* (?:>=|<=|>|<|=) -?[0-9]+(?:\.[0-9]+)?$")


@dataclass(frozen=True)
class Check:
    id: str
    type: str
    table: str | None = None
    column: str | None = None
    min_rows: int | None = None
    max_lag_hours: int | None = None
    sum_table: str | None = None
    sum_column: str | None = None
    count_table: str | None = None
    filter_table: str | None = None
    where: str | None = None


@dataclass(frozen=True)
class CheckOutcome:
    check_id: str
    passed: bool
    detail: str


def load_checks(path: Path | None = None) -> list[Check]:
    source = path or SCHEMA_PATH
    payload = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
    raw_checks = payload.get("checks")
    if not isinstance(raw_checks, list) or not raw_checks:
        raise PipelineError(f"{source} must contain a non-empty checks list")
    checks: list[Check] = []
    seen: set[str] = set()
    for item in raw_checks:
        if not isinstance(item, dict):
            raise PipelineError(f"{source} has a check that is not a mapping")
        check = _parse_check(item)
        if check.id in seen:
            raise PipelineError(f"duplicate quality check id: {check.id}")
        seen.add(check.id)
        checks.append(check)
    return checks


def evaluate(
    settings: Settings,
    *,
    now: datetime | None = None,
    checks_path: Path | None = None,
) -> list[CheckOutcome]:
    clock = naive_utc(now or datetime.now(UTC))
    checks = load_checks(checks_path)
    warehouse = open_warehouse(settings)
    try:
        return [_run_check(warehouse, check, clock) for check in checks]
    finally:
        warehouse.close()


def enforce(
    settings: Settings,
    load_id: str,
    *,
    now: datetime | None = None,
    checks_path: Path | None = None,
) -> list[CheckOutcome]:
    """Run every check, record the audit status, and raise if any check failed."""

    outcomes = evaluate(settings, now=now, checks_path=checks_path)
    failures = [item for item in outcomes if not item.passed]
    _set_audit_status(settings, load_id, "quality_failed" if failures else "success")
    if failures:
        raise DataQualityError(failures)
    return outcomes


def _set_audit_status(settings: Settings, load_id: str, status: str) -> None:
    warehouse = open_warehouse(settings)
    try:
        warehouse.begin()
        warehouse.execute(
            "UPDATE meta.load_audit "
            f"SET status = {warehouse.placeholder} "
            f"WHERE load_id = {warehouse.placeholder}",
            [status, load_id],
        )
        warehouse.commit()
    except Exception:
        warehouse.rollback()
        raise
    finally:
        warehouse.close()


def _run_check(warehouse: Warehouse, check: Check, now: datetime) -> CheckOutcome:
    if check.type == "min_rows":
        count = _query_int(warehouse, f"SELECT COUNT(*) FROM {_table(check.table)}")
        minimum = check.min_rows if check.min_rows is not None else 1
        return CheckOutcome(
            check.id,
            count >= minimum,
            f"rows={count} min_rows={minimum}",
        )
    if check.type == "not_null":
        table = _table(check.table)
        column = _ident(check.column)
        count = _query_int(
            warehouse,
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE {column} IS NULL OR TRIM(CAST({column} AS VARCHAR)) = ''",
        )
        return CheckOutcome(check.id, count == 0, f"nulls={count}")
    if check.type == "unique":
        table = _table(check.table)
        column = _ident(check.column)
        count = _query_int(
            warehouse,
            "SELECT COUNT(*) FROM ("
            f"SELECT TRIM(CAST({column} AS VARCHAR)) AS value FROM {table} "
            f"WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS VARCHAR)) <> '' "
            "GROUP BY 1 HAVING COUNT(*) > 1"
            ") duplicates",
        )
        return CheckOutcome(check.id, count == 0, f"duplicate_keys={count}")
    if check.type == "freshness":
        return _freshness(warehouse, check, now)
    if check.type == "sum_equals_count":
        total = _query_int(
            warehouse,
            f"SELECT COALESCE(SUM({_ident(check.sum_column)}), 0) FROM {_table(check.sum_table)}",
        )
        count = _query_int(warehouse, f"SELECT COUNT(*) FROM {_table(check.count_table)}")
        return CheckOutcome(check.id, total == count, f"sum={total} count={count}")
    if check.type == "count_equals_filter":
        where = _where(check.where)
        left = _query_int(warehouse, f"SELECT COUNT(*) FROM {_table(check.table)}")
        right = _query_int(
            warehouse,
            f"SELECT COUNT(*) FROM {_table(check.filter_table)} WHERE {where}",
        )
        return CheckOutcome(check.id, left == right, f"rows={left} filtered={right}")
    raise PipelineError(f"unknown quality check type: {check.type}")


def _freshness(warehouse: Warehouse, check: Check, now: datetime) -> CheckOutcome:
    table = _table(check.table)
    column = _ident(check.column)
    row = warehouse.fetchone(f"SELECT MAX({column}) FROM {table}")
    max_at = row[0] if row else None
    if isinstance(max_at, str):
        max_at = parse_timestamp(max_at)
    if max_at is None:
        return CheckOutcome(check.id, False, "max_timestamp is null")
    if not isinstance(max_at, datetime):
        return CheckOutcome(
            check.id, False, f"max_timestamp has unexpected type {type(max_at).__name__}"
        )
    lag = now - naive_utc(max_at)
    limit_hours = check.max_lag_hours if check.max_lag_hours is not None else 36
    limit = timedelta(hours=limit_hours)
    lag_seconds = int(lag.total_seconds())
    detail = (
        f"max_extracted_at={naive_utc(max_at).isoformat(sep=' ', timespec='seconds')} "
        f"lag_seconds={lag_seconds} max_lag_hours={limit_hours}"
    )
    return CheckOutcome(check.id, lag <= limit, detail)


def _parse_check(item: dict) -> Check:
    check_id = str(item.get("id") or "").strip()
    check_type = str(item.get("type") or "").strip()
    if not check_id or not _IDENT.match(check_id):
        raise PipelineError(f"quality check id is invalid: {check_id!r}")
    if check_type not in {
        "min_rows",
        "not_null",
        "unique",
        "freshness",
        "sum_equals_count",
        "count_equals_filter",
    }:
        raise PipelineError(f"unknown quality check type on {check_id}: {check_type}")
    check = Check(
        id=check_id,
        type=check_type,
        table=_optional_table(item.get("table"), check_id),
        column=_optional_ident(item.get("column"), check_id, "column"),
        min_rows=_optional_int(item.get("min_rows"), check_id, "min_rows"),
        max_lag_hours=_optional_int(item.get("max_lag_hours"), check_id, "max_lag_hours"),
        sum_table=_optional_table(item.get("sum_table"), check_id),
        sum_column=_optional_ident(item.get("sum_column"), check_id, "sum_column"),
        count_table=_optional_table(item.get("count_table"), check_id),
        filter_table=_optional_table(item.get("filter_table"), check_id),
        where=_optional_where(item.get("where"), check_id),
    )
    _require_fields(check)
    return check


def _require_fields(check: Check) -> None:
    required: dict[str, tuple[str, ...]] = {
        "min_rows": ("table", "min_rows"),
        "not_null": ("table", "column"),
        "unique": ("table", "column"),
        "freshness": ("table", "column", "max_lag_hours"),
        "sum_equals_count": ("sum_table", "sum_column", "count_table"),
        "count_equals_filter": ("table", "filter_table", "where"),
    }
    for field in required[check.type]:
        if getattr(check, field) is None:
            raise PipelineError(f"quality check {check.id} is missing {field}")


def _optional_table(value: object, check_id: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not _TABLE.match(text):
        raise PipelineError(f"quality check {check_id} has an invalid table name: {text!r}")
    return text


def _optional_ident(value: object, check_id: str, field: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not _IDENT.match(text):
        raise PipelineError(f"quality check {check_id} has an invalid {field}: {text!r}")
    return text


def _optional_where(value: object, check_id: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not _WHERE.match(text):
        raise PipelineError(f"quality check {check_id} has an invalid where clause: {text!r}")
    return text


def _optional_int(value: object, check_id: str, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineError(f"quality check {check_id} {field} must be an integer")
    return value


def _table(value: str | None) -> str:
    if value is None or not _TABLE.match(value):
        raise PipelineError(f"invalid table name: {value!r}")
    return value


def _ident(value: str | None) -> str:
    if value is None or not _IDENT.match(value):
        raise PipelineError(f"invalid identifier: {value!r}")
    return value


def _where(value: str | None) -> str:
    if value is None or not _WHERE.match(value):
        raise PipelineError(f"invalid where clause: {value!r}")
    return value


def _query_int(warehouse: Warehouse, sql: str) -> int:
    row = warehouse.fetchone(sql)
    if row is None or row[0] is None:
        return 0
    value = row[0]
    if isinstance(value, bool):
        raise PipelineError(f"expected a number from [{sql}], got a boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise PipelineError(f"expected a whole number from [{sql}], got {value}")
        return int(value)
    text = str(value)
    if "." in text:
        whole, fraction = text.split(".", 1)
        if fraction.strip("0") != "":
            raise PipelineError(f"expected a whole number from [{sql}], got {value}")
        return int(whole)
    return int(text)
