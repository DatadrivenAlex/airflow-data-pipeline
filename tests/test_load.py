from pipeline.extract import extract_csv_file
from pipeline.load import load_extract
from tests.conftest import JAN1, JAN2, JAN3, feed_row, fetch_all, write_feed


def test_replaying_a_file_does_not_duplicate_rows(settings, tmp_path):
    path = tmp_path / "day.csv"
    write_feed(
        path,
        [
            feed_row("a", "2024-01-01T01:00:00Z", "4.6", "10 km N of A, Japan"),
            feed_row("b", "2024-01-01T02:00:00Z", "4.8", "South of the Fiji Islands"),
        ],
    )
    extracted = extract_csv_file(settings, path, JAN1, JAN2, overwrite_window=False)
    first = load_extract(settings, extracted)
    second = load_extract(settings, extracted)
    assert first.raw_rows == 2
    assert second.raw_rows == 2
    assert second.rows_landed == 2


def test_a_revised_magnitude_replaces_the_event(settings, tmp_path):
    first_path = tmp_path / "first.csv"
    second_path = tmp_path / "second.csv"
    write_feed(
        first_path,
        [
            feed_row(
                "a",
                "2024-01-01T01:00:00Z",
                "4.6",
                "10 km N of A, Japan",
                updated="2024-01-01T02:00:00Z",
            )
        ],
    )
    write_feed(
        second_path,
        [
            feed_row(
                "a",
                "2024-01-01T01:00:00Z",
                "5.2",
                "10 km N of A, Japan",
                updated="2024-01-02T02:00:00Z",
            )
        ],
    )
    load_extract(
        settings, extract_csv_file(settings, first_path, JAN1, JAN2, overwrite_window=True)
    )
    loaded = load_extract(
        settings, extract_csv_file(settings, second_path, JAN1, JAN2, overwrite_window=True)
    )
    assert loaded.raw_rows == 1
    assert fetch_all(settings, "SELECT mag_text FROM raw.earthquakes") == [("5.2",)]


def test_blank_event_id_is_not_appended_on_replay(settings, tmp_path):
    path = tmp_path / "blank.csv"
    write_feed(
        path,
        [
            feed_row("ok", "2024-01-01T01:00:00Z", "4.8", "10 km N of A, Japan"),
            feed_row("", "2024-01-01T02:00:00Z", "4.8", "10 km N of B, Japan"),
        ],
    )
    extracted = extract_csv_file(settings, path, JAN1, JAN2, overwrite_window=False)
    load_extract(settings, extracted)
    load_extract(settings, extracted)
    rows = fetch_all(
        settings,
        "SELECT event_id FROM raw.earthquakes ORDER BY event_id NULLS FIRST",
    )
    assert rows == [(None,), ("ok",)]


def test_window_overwrite_drops_missing_events_and_keeps_other_days(settings, tmp_path):
    day1 = tmp_path / "day1.csv"
    day1_revised = tmp_path / "day1b.csv"
    day2 = tmp_path / "day2.csv"
    write_feed(
        day1,
        [
            feed_row("a", "2024-01-01T01:00:00Z", "4.6", "10 km N of A, Japan"),
            feed_row("b", "2024-01-01T06:00:00Z", "4.7", "10 km N of B, Japan"),
        ],
    )
    write_feed(day1_revised, [feed_row("a", "2024-01-01T01:00:00Z", "4.6", "10 km N of A, Japan")])
    write_feed(day2, [feed_row("c", "2024-01-02T03:00:00Z", "5.1", "South of the Fiji Islands")])

    load_extract(settings, extract_csv_file(settings, day1, JAN1, JAN2, overwrite_window=True))
    load_extract(settings, extract_csv_file(settings, day2, JAN2, JAN3, overwrite_window=True))
    load_extract(
        settings, extract_csv_file(settings, day1_revised, JAN1, JAN2, overwrite_window=True)
    )

    ids = [
        row[0]
        for row in fetch_all(settings, "SELECT event_id FROM raw.earthquakes ORDER BY event_id")
    ]
    assert ids == ["a", "c"]
