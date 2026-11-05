from datetime import datetime
from pathlib import Path

from pipeline.config import SAMPLE_WINDOW_END, SAMPLE_WINDOW_START
from pipeline.service import run_elt
from pipeline.transform import TRANSFORMS_DIR
from tests.conftest import JAN1, JAN2, JAN3, feed_row, fetch_all, write_feed
from tests.oracle import catalog_rows, daily_counts, region_counts


def test_sql_models_are_single_selects():
    staging = sorted((TRANSFORMS_DIR / "staging").glob("*.sql"))
    marts = sorted((TRANSFORMS_DIR / "marts").glob("*.sql"))
    assert [path.name for path in staging] == ["stg_earthquakes.sql"]
    assert [path.name for path in marts] == [
        "mart_daily_seismicity.sql",
        "mart_region_summary.sql",
        "mart_significant_events.sql",
    ]
    for path in staging + marts:
        text = path.read_text(encoding="utf-8")
        assert ";" not in text
        assert "raw.earthquakes" in text or "staging.stg_earthquakes" in text


def test_region_parsing_and_magnitude_bands(settings, tmp_path):
    path = tmp_path / "bands.csv"
    write_feed(
        path,
        [
            feed_row("light", "2024-01-01T01:00:00Z", "4.6", "10 km N of Hihifo, Tonga"),
            feed_row("moderate", "2024-01-01T02:00:00Z", "5.1", "15 km SSE of Layton, Utah"),
            feed_row("strong", "2024-01-01T03:00:00Z", "6.2", "South of the Fiji Islands"),
            feed_row("major", "2024-01-01T04:00:00Z", "7.1", "20 km NE of Anamizu, Japan"),
        ],
    )
    run_elt(settings.with_overrides(min_magnitude=0), JAN1, JAN2, source_csv=path)
    rows = fetch_all(
        settings,
        "SELECT event_id, region, magnitude_band FROM staging.stg_earthquakes ORDER BY event_id",
    )
    assert rows == [
        ("light", "Tonga", "light"),
        ("major", "Japan", "major"),
        ("moderate", "Utah", "moderate"),
        ("strong", "South of the Fiji Islands", "strong"),
    ]


def test_non_earthquake_and_stale_revision_are_dropped(settings, tmp_path):
    path = tmp_path / "mixed.csv"
    write_feed(
        path,
        [
            feed_row(
                "a",
                "2024-01-01T01:00:00Z",
                "5.0",
                "10 km N of A, Japan",
                updated="2024-01-01T01:00:00Z",
            ),
            feed_row(
                "a",
                "2024-01-01T01:00:00Z",
                "1.2",
                "10 km N of A, Japan",
                updated="2024-01-03T01:00:00Z",
                event_type="quarry blast",
            ),
            feed_row("b", "2024-01-01T04:00:00Z", "4.8", "South of the Fiji Islands"),
            feed_row(
                "c",
                "2024-01-01T05:00:00Z",
                "4.9",
                "10 km N of C, Japan",
                event_type="explosion",
            ),
        ],
    )
    report = run_elt(settings.with_overrides(min_magnitude=0), JAN1, JAN2, source_csv=path)
    assert report.staging_rows == 1
    assert fetch_all(settings, "SELECT event_id FROM staging.stg_earthquakes") == [("b",)]


def test_duplicate_rows_in_one_file_collapse_before_the_mart(settings, tmp_path):
    path = tmp_path / "dupes.csv"
    write_feed(
        path,
        [
            feed_row(
                "a",
                "2024-01-01T01:00:00Z",
                "4.4",
                "10 km N of A, Japan",
                updated="2024-01-01T01:00:00Z",
            ),
            feed_row(
                "a",
                "2024-01-01T01:00:00Z",
                "6.1",
                "10 km N of A, Japan",
                updated="2024-01-02T01:00:00Z",
            ),
        ],
    )
    report = run_elt(settings.with_overrides(min_magnitude=0), JAN1, JAN2, source_csv=path)
    assert report.rows_in_file == 2
    assert report.rows_landed == 1
    assert report.staging_rows == 1
    assert fetch_all(settings, "SELECT magnitude_band FROM staging.stg_earthquakes") == [
        ("strong",)
    ]


def test_bundled_sample_matches_an_independent_aggregation(settings):
    expected = catalog_rows(settings.sample_csv, min_magnitude=4.5)
    report = run_elt(settings, SAMPLE_WINDOW_START, SAMPLE_WINDOW_END)
    assert report.source == "sample"
    assert report.staging_rows == len(expected)
    assert report.raw_rows == len(expected)
    assert _daily(settings) == daily_counts(expected)
    assert _regions(settings) == region_counts(expected)
    assert report.significant_rows == sum(
        1 for row in expected if row["magnitude"] is not None and float(row["magnitude"]) >= 5
    )
    assert float(report.max_magnitude) == max(float(row["magnitude"]) for row in expected)


def test_a_higher_magnitude_floor_filters_the_bundled_sample(settings):
    expected = catalog_rows(settings.sample_csv, min_magnitude=6)
    report = run_elt(
        settings.with_overrides(min_magnitude=6),
        SAMPLE_WINDOW_START,
        SAMPLE_WINDOW_END,
    )
    assert report.staging_rows == len(expected)
    assert _daily(settings) == daily_counts(expected)


def test_incremental_windows_accumulate_in_the_daily_mart(settings, tmp_path: Path):
    day1 = tmp_path / "day1.csv"
    day2 = tmp_path / "day2.csv"
    write_feed(
        day1,
        [
            feed_row("a", "2024-01-01T01:00:00Z", "4.6", "10 km N of A, Japan"),
            feed_row("b", "2024-01-01T08:00:00Z", "5.4", "10 km N of B, Japan"),
        ],
    )
    write_feed(day2, [feed_row("c", "2024-01-02T03:00:00Z", "4.9", "South of the Fiji Islands")])
    tuned = settings.with_overrides(min_magnitude=0, offline=False)
    run_elt(tuned, JAN1, JAN2, source_csv=day1)
    report = run_elt(tuned, JAN2, JAN3, source_csv=day2)
    assert report.raw_rows == 3
    assert _daily(settings) == {"2024-01-01": 2, "2024-01-02": 1}
    assert report.significant_rows == 1

    revised = tmp_path / "day1b.csv"
    write_feed(revised, [feed_row("a", "2024-01-01T01:00:00Z", "4.6", "10 km N of A, Japan")])
    run_elt(tuned, JAN1, JAN2, source_csv=revised)
    assert _daily(settings) == {"2024-01-01": 1, "2024-01-02": 1}


def _daily(settings) -> dict[str, int]:
    rows = fetch_all(settings, "SELECT event_date, event_count FROM marts.mart_daily_seismicity")
    return {_as_date(day): int(count) for day, count in rows}


def _regions(settings) -> dict[str, int]:
    rows = fetch_all(settings, "SELECT region, event_count FROM marts.mart_region_summary")
    return {str(region): int(count) for region, count in rows}


def _as_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    return str(value)[:10]
