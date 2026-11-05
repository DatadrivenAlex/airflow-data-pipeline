import os
from pathlib import Path

import pytest

from pipeline.service import run_elt
from pipeline.warehouse import open_warehouse, reset_storage
from tests.conftest import JAN1, JAN2, JAN3, feed_row, make_settings, write_feed
from tests.oracle import catalog_rows, daily_counts

pytestmark = pytest.mark.postgres


@pytest.fixture
def postgres_settings(tmp_path: Path):
    url = os.getenv("POSTGRES_TEST_URL")
    if not url:
        pytest.skip("POSTGRES_TEST_URL is not set")
    pytest.importorskip("psycopg2")
    settings = make_settings(tmp_path, warehouse_url=url, offline=True)
    _reset(settings)
    yield settings
    _reset(settings)


def test_postgres_sample_matches_the_independent_aggregation(postgres_settings):
    from pipeline.config import SAMPLE_WINDOW_END, SAMPLE_WINDOW_START

    expected = catalog_rows(postgres_settings.sample_csv, min_magnitude=4.5)
    report = run_elt(postgres_settings, SAMPLE_WINDOW_START, SAMPLE_WINDOW_END)
    assert report.staging_rows == len(expected)
    assert report.audit_status == "success"
    assert _daily(postgres_settings) == daily_counts(expected)


def test_postgres_window_overwrite_is_idempotent(postgres_settings, tmp_path: Path):
    day1 = tmp_path / "day1.csv"
    day2 = tmp_path / "day2.csv"
    write_feed(
        day1,
        [
            feed_row("a", "2024-01-01T01:00:00Z", "4.6", "10 km N of A, Japan"),
            feed_row("b", "2024-01-01T06:00:00Z", "5.2", "10 km N of B, Japan"),
        ],
    )
    write_feed(day2, [feed_row("c", "2024-01-02T03:00:00Z", "4.9", "South of the Fiji Islands")])
    tuned = postgres_settings.with_overrides(min_magnitude=0, offline=False)
    run_elt(tuned, JAN1, JAN2, source_csv=day1)
    first = run_elt(tuned, JAN2, JAN3, source_csv=day2)
    second = run_elt(tuned, JAN2, JAN3, source_csv=day2)
    assert first.raw_rows == 3
    assert second.raw_rows == 3
    assert _daily(postgres_settings) == {"2024-01-01": 2, "2024-01-02": 1}


def _reset(settings) -> None:
    warehouse = open_warehouse(settings)
    try:
        warehouse.begin()
        reset_storage(warehouse)
        warehouse.commit()
    except Exception:
        warehouse.rollback()
        raise
    finally:
        warehouse.close()


def _daily(settings) -> dict[str, int]:
    warehouse = open_warehouse(settings)
    try:
        rows = warehouse.fetchall("SELECT event_date, event_count FROM marts.mart_daily_seismicity")
    finally:
        warehouse.close()
    return {str(day)[:10]: int(count) for day, count in rows}
