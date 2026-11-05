from datetime import datetime

import pytest

from pipeline.errors import DataQualityError, PipelineError
from pipeline.quality import enforce, evaluate, load_checks
from pipeline.service import run_elt
from tests.conftest import JAN1, JAN2, feed_row, fetch_all, write_feed


def _clean_csv(tmp_path):
    path = tmp_path / "clean.csv"
    write_feed(
        path,
        [
            feed_row("a", "2024-01-01T01:00:00Z", "4.8", "10 km N of A, Japan"),
            feed_row("b", "2024-01-01T05:00:00Z", "5.6", "South of the Fiji Islands"),
        ],
    )
    return path


def test_schema_declares_row_null_unique_freshness_and_mart_checks():
    checks = load_checks()
    assert {check.type for check in checks} == {
        "min_rows",
        "not_null",
        "unique",
        "freshness",
        "sum_equals_count",
        "count_equals_filter",
    }
    assert len(checks) == 13


def test_invalid_check_definitions_are_rejected(tmp_path):
    unknown = tmp_path / "unknown.yml"
    unknown.write_text(
        "checks:\n  - id: bad\n    type: stats\n    table: staging.stg_earthquakes\n",
        encoding="utf-8",
    )
    duplicate = tmp_path / "duplicate.yml"
    duplicate.write_text(
        "checks:\n"
        "  - id: same\n    type: min_rows\n    table: staging.stg_earthquakes\n    min_rows: 1\n"
        "  - id: same\n    type: min_rows\n    table: staging.stg_earthquakes\n    min_rows: 1\n",
        encoding="utf-8",
    )
    injected = tmp_path / "injected.yml"
    injected.write_text(
        "\n".join(
            [
                "checks:",
                "  - id: bad",
                "    type: min_rows",
                "    table: staging.stg_earthquakes;drop",
                "    min_rows: 1",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(PipelineError, match="unknown quality check type"):
        load_checks(unknown)
    with pytest.raises(PipelineError, match="duplicate"):
        load_checks(duplicate)
    with pytest.raises(PipelineError, match="invalid table"):
        load_checks(injected)


def test_clean_load_passes_and_marks_the_audit_successful(settings, tmp_path):
    report = run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    assert report.audit_status == "success"
    assert all(check.passed for check in report.checks)
    assert [row[0] for row in fetch_all(settings, "SELECT status FROM meta.load_audit")] == [
        "success"
    ]


def test_null_latitude_fails_the_not_null_check(settings, tmp_path):
    run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    _execute(settings, "UPDATE staging.stg_earthquakes SET latitude = NULL WHERE event_id = 'a'")
    failed = _failed_ids(settings)
    assert failed == {"staging_latitude_not_null"}


def test_duplicate_event_id_fails_uniqueness(settings, tmp_path):
    run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    _execute(
        settings,
        "INSERT INTO staging.stg_earthquakes "
        "SELECT * FROM staging.stg_earthquakes WHERE event_id = 'a'",
    )
    failed = _failed_ids(settings)
    assert "staging_event_id_unique" in failed
    assert "daily_mart_reconciles" in failed


def test_empty_staging_fails_the_row_count_check(settings, tmp_path):
    run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    _execute(settings, "DELETE FROM staging.stg_earthquakes")
    failed = _failed_ids(settings)
    assert "staging_min_rows" in failed


def test_stale_extract_fails_freshness(settings, tmp_path):
    run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    _execute(
        settings,
        "UPDATE staging.stg_earthquakes SET extracted_at = TIMESTAMP '2020-01-01 00:00:00'",
    )
    outcomes = evaluate(settings, now=datetime(2024, 6, 1))
    freshness = next(item for item in outcomes if item.check_id == "staging_freshness")
    assert freshness.passed is False
    assert "lag_seconds=" in freshness.detail
    assert [item.check_id for item in outcomes if not item.passed] == ["staging_freshness"]


def test_mart_sum_mismatch_fails_reconciliation(settings, tmp_path):
    run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    _execute(settings, "UPDATE marts.mart_daily_seismicity SET event_count = event_count + 5")
    failed = _failed_ids(settings)
    assert failed == {"daily_mart_reconciles"}


def test_one_exception_lists_every_failed_check(settings, tmp_path):
    run_elt(
        settings.with_overrides(min_magnitude=0),
        JAN1,
        JAN2,
        source_csv=_clean_csv(tmp_path),
    )
    _execute(
        settings,
        "UPDATE staging.stg_earthquakes "
        "SET latitude = NULL, extracted_at = TIMESTAMP '2020-01-01 00:00:00'",
    )
    with pytest.raises(DataQualityError) as caught:
        enforce(settings, "ignored", now=datetime(2024, 6, 1))
    failed = {item.check_id for item in caught.value.failures}
    assert {"staging_latitude_not_null", "staging_freshness"} <= failed
    assert "staging_latitude_not_null" in str(caught.value)
    assert "staging_freshness" in str(caught.value)


def test_blank_magnitude_fails_the_run_and_marks_the_audit(settings, tmp_path):
    path = tmp_path / "blank-mag.csv"
    write_feed(path, [feed_row("a", "2024-01-01T01:00:00Z", "", "10 km N of A, Japan")])
    with pytest.raises(DataQualityError) as caught:
        run_elt(settings.with_overrides(min_magnitude=0), JAN1, JAN2, source_csv=path)
    assert "staging_magnitude_not_null" in {item.check_id for item in caught.value.failures}
    assert fetch_all(settings, "SELECT status FROM meta.load_audit") == [("quality_failed",)]


def _execute(settings, sql: str) -> None:
    from pipeline.warehouse import open_warehouse

    warehouse = open_warehouse(settings)
    try:
        warehouse.begin()
        warehouse.execute(sql)
        warehouse.commit()
    except Exception:
        warehouse.rollback()
        raise
    finally:
        warehouse.close()


def _failed_ids(settings, now: datetime | None = None) -> set[str]:
    outcomes = evaluate(settings, now=now or datetime(2026, 1, 1))
    return {item.check_id for item in outcomes if not item.passed}
