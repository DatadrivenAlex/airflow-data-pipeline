import urllib.parse
from datetime import datetime

import pytest

from pipeline.errors import ExtractError
from pipeline.extract import (
    SOURCE_API,
    SOURCE_SAMPLE,
    extract_csv_file,
    extract_step,
    usgs_query_url,
)
from pipeline.records import CSV_COLUMNS, read_events
from tests.conftest import JAN1, JAN2, feed_row, write_feed
from tests.oracle import catalog_rows


def test_usgs_url_uses_a_half_open_window_and_magnitude_floor():
    url = usgs_query_url(datetime(2024, 1, 1), datetime(2024, 2, 1), 4.5)
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    assert parsed.netloc == "earthquake.usgs.gov"
    assert parsed.path == "/fdsnws/event/1/query"
    assert params["format"] == ["csv"]
    assert params["starttime"] == ["2024-01-01T00:00:00"]
    assert params["endtime"] == ["2024-02-01T00:00:00"]
    assert params["minmagnitude"] == ["4.5"]
    assert params["orderby"] == ["time-asc"]


def test_offline_extract_does_not_touch_the_network(settings):
    def downloader(url: str, timeout: int) -> bytes:
        raise AssertionError(f"offline extract called the network: {url} timeout={timeout}")

    result = extract_step(settings, JAN1, datetime(2024, 2, 1), downloader=downloader)
    assert result.source == SOURCE_SAMPLE
    assert result.overwrite_window is False
    assert result.row_count == len(catalog_rows(settings.sample_csv, min_magnitude=4.5))


def test_api_success_writes_the_response_and_overwrites_the_window(settings):
    body = (",".join(CSV_COLUMNS) + "\n").encode()
    seen: dict[str, object] = {}

    def downloader(url: str, timeout: int) -> bytes:
        seen["url"] = url
        seen["timeout"] = timeout
        return body

    result = extract_step(
        settings.with_overrides(offline=False),
        JAN1,
        JAN2,
        downloader=downloader,
    )
    assert result.source == SOURCE_API
    assert result.overwrite_window is True
    assert result.row_count == 0
    assert seen["timeout"] == settings.request_timeout_seconds
    assert str(seen["url"]).startswith("https://earthquake.usgs.gov/")
    roundtrip = result.to_dict()
    restored = type(result).from_dict(roundtrip)
    assert restored == result


def test_api_failure_falls_back_to_the_sample_without_window_overwrite(settings):
    def downloader(url: str, timeout: int) -> bytes:
        raise ExtractError("timeout talking to USGS")

    result = extract_step(
        settings.with_overrides(offline=False),
        JAN1,
        JAN2,
        downloader=downloader,
    )
    assert result.source == SOURCE_SAMPLE
    assert result.overwrite_window is False
    assert result.row_count > 0


def test_api_failure_is_raised_when_fallback_is_disabled(settings):
    def downloader(url: str, timeout: int) -> bytes:
        raise ExtractError("HTTP 503")

    with pytest.raises(ExtractError, match="503"):
        extract_step(
            settings.with_overrides(offline=False, allow_sample_fallback=False),
            JAN1,
            JAN2,
            downloader=downloader,
        )


def test_non_csv_response_uses_the_sample(settings):
    def downloader(url: str, timeout: int) -> bytes:
        return b"<html>unavailable</html>"

    result = extract_step(
        settings.with_overrides(offline=False),
        JAN1,
        JAN2,
        downloader=downloader,
    )
    assert result.source == SOURCE_SAMPLE


def test_from_csv_keeps_only_the_requested_window(settings, tmp_path):
    path = tmp_path / "local.csv"
    write_feed(
        path,
        [
            feed_row("a", "2024-01-01T01:00:00Z", "4.8", "10 km N of A, Japan"),
            feed_row("b", "2024-01-02T01:00:00Z", "4.8", "10 km N of B, Japan"),
        ],
    )
    result = extract_csv_file(settings, path, JAN1, JAN2, overwrite_window=True)
    assert result.overwrite_window is True
    assert [event.event_id for event in read_events(result.extract_path)] == ["a"]
    assert result.window_end == JAN2
    assert result.window_start == JAN1
