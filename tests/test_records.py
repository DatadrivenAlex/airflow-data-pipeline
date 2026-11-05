from datetime import datetime

from pipeline.errors import LoadError
from pipeline.records import (
    collapse_latest,
    event_from_mapping,
    filter_events,
    parse_float,
    parse_timestamp,
    read_events,
    write_events,
)
from tests.conftest import feed_row, write_feed


def test_parse_usgs_timestamp():
    parsed = parse_timestamp("2024-02-01T07:45:38.911Z")
    assert parsed == datetime(2024, 2, 1, 7, 45, 38, 911000)
    assert parse_timestamp("") is None
    assert parse_timestamp("not-a-time") is None
    assert parse_float("5.4") == 5.4
    assert parse_float("") is None
    assert parse_float("mb") is None


def test_collapse_keeps_the_latest_revision_and_the_later_tie():
    older = event_from_mapping(
        feed_row(
            "a",
            "2024-01-01T00:00:00Z",
            "4.4",
            "10 km N of A, Japan",
            updated="2024-01-01T00:00:00Z",
        )
    )
    newer = event_from_mapping(
        feed_row(
            "a",
            "2024-01-01T00:00:00Z",
            "6.1",
            "10 km N of A, Japan",
            updated="2024-01-02T00:00:00Z",
        )
    )
    tied_first = event_from_mapping(
        feed_row(
            "b",
            "2024-01-01T00:00:00Z",
            "4.5",
            "South of the Fiji Islands",
            updated="2024-01-01T03:00:00Z",
        )
    )
    tied_last = event_from_mapping(
        feed_row(
            "b",
            "2024-01-01T00:00:00Z",
            "4.9",
            "South of the Fiji Islands",
            updated="2024-01-01T03:00:00Z",
        )
    )
    collapsed = {
        event.event_id: event for event in collapse_latest([older, newer, tied_first, tied_last])
    }
    assert collapsed["a"].mag_text == "6.1"
    assert collapsed["b"].mag_text == "4.9"


def test_blank_ids_are_all_retained():
    blank = event_from_mapping(feed_row("", "2024-01-01T00:00:00Z", "4.6", "10 km N of A, Japan"))
    other = event_from_mapping(feed_row("", "2024-01-01T02:00:00Z", "4.7", "10 km N of B, Japan"))
    collapsed = collapse_latest([blank, other])
    assert len(collapsed) == 2
    assert all(event.event_id is None for event in collapsed)


def test_filter_window_is_half_open_and_keeps_unparsed_magnitude():
    inside = event_from_mapping(feed_row("a", "2024-01-01T23:00:00Z", "4.6", "10 km N of A, Japan"))
    end = event_from_mapping(feed_row("b", "2024-01-02T00:00:00Z", "4.6", "10 km N of B, Japan"))
    missing_mag = event_from_mapping(
        feed_row("c", "2024-01-01T01:00:00Z", "", "10 km N of C, Japan")
    )
    selected = filter_events(
        [inside, end, missing_mag],
        min_magnitude=4.5,
        window_start=datetime(2024, 1, 1),
        window_end=datetime(2024, 1, 2),
    )
    assert [event.event_id for event in selected] == ["a", "c"]


def test_csv_roundtrip_preserves_commas_in_place(tmp_path):
    path = tmp_path / "feed.csv"
    write_feed(path, [feed_row("a", "2024-01-01T01:00:00Z", "5.1", "116 km NE of Hihifo, Tonga")])
    events = read_events(path)
    assert events[0].place_text == "116 km NE of Hihifo, Tonga"
    copy = tmp_path / "copy.csv"
    write_events(copy, events)
    assert read_events(copy)[0].place_text == "116 km NE of Hihifo, Tonga"


def test_missing_required_column_raises(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("time,latitude\n2024-01-01T00:00:00Z,1\n", encoding="utf-8")
    try:
        read_events(path)
    except LoadError as exc:
        assert "missing required USGS columns" in str(exc)
    else:
        raise AssertionError("expected LoadError")
