"""Extract one USGS interval, with a bundled CSV used offline and as a fallback."""

from __future__ import annotations

import logging
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pipeline.config import Settings
from pipeline.errors import ExtractError
from pipeline.records import (
    filter_events,
    naive_utc,
    parse_timestamp,
    read_events,
    write_events,
)

logger = logging.getLogger(__name__)

USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USER_AGENT = "airflow-data-pipeline/1.0 (educational ELT; offline sample fallback)"
MAX_RESPONSE_BYTES = 50 * 1024 * 1024

SOURCE_API = "api"
SOURCE_SAMPLE = "sample"
SOURCE_FILE = "file"

Downloader = Callable[[str, int], bytes]


@dataclass(frozen=True)
class ExtractResult:
    extract_path: str
    source: str
    row_count: int
    window_start: datetime
    window_end: datetime
    overwrite_window: bool
    min_magnitude: float

    def to_dict(self) -> dict[str, object]:
        return {
            "extract_path": self.extract_path,
            "source": self.source,
            "row_count": self.row_count,
            "window_start": _stamp(self.window_start),
            "window_end": _stamp(self.window_end),
            "overwrite_window": self.overwrite_window,
            "min_magnitude": self.min_magnitude,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ExtractResult:
        return cls(
            extract_path=str(payload["extract_path"]),
            source=str(payload["source"]),
            row_count=int(str(payload["row_count"])),
            window_start=_require_timestamp(payload["window_start"]),
            window_end=_require_timestamp(payload["window_end"]),
            overwrite_window=_as_bool(payload["overwrite_window"]),
            min_magnitude=float(str(payload["min_magnitude"])),
        )


def usgs_query_url(window_start: datetime, window_end: datetime, min_magnitude: float) -> str:
    query = urllib.parse.urlencode(
        {
            "format": "csv",
            "starttime": naive_utc(window_start).strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": naive_utc(window_end).strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": f"{min_magnitude:g}",
            "orderby": "time-asc",
        }
    )
    return f"{USGS_ENDPOINT}?{query}"


def extract_step(
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
    *,
    downloader: Downloader | None = None,
) -> ExtractResult:
    """Extract the requested window.

    Offline mode replays the bundled sample and does not delete a warehouse
    partition. Live mode downloads the USGS CSV for the window and replaces
    that partition on load. A failed download uses the sample when fallback is
    enabled, again without deleting the requested partition.
    """

    window_start = naive_utc(window_start)
    window_end = naive_utc(window_end)
    _require_window(window_start, window_end)
    if settings.offline:
        destination = _destination(settings, SOURCE_SAMPLE, window_start, window_end)
        return _materialize_sample(settings, destination, window_start, window_end)

    destination = _destination(settings, SOURCE_API, window_start, window_end)
    fetcher = downloader or download_usgs_csv
    url = usgs_query_url(window_start, window_end, settings.min_magnitude)
    try:
        payload = fetcher(url, settings.request_timeout_seconds)
        if len(payload) > MAX_RESPONSE_BYTES:
            raise ExtractError("USGS response exceeded 50 MB")
        if not looks_like_event_csv(payload):
            raise ExtractError("USGS response was not a CSV event feed")
    except Exception as exc:
        if not settings.allow_sample_fallback:
            if isinstance(exc, ExtractError):
                raise
            raise ExtractError(f"USGS extract failed: {exc}") from exc
        logger.warning("USGS extract failed (%s); loading the bundled sample", exc)
        sample_path = _destination(settings, SOURCE_SAMPLE, window_start, window_end)
        return _materialize_sample(settings, sample_path, window_start, window_end)

    _write_bytes(destination, payload)
    row_count = len(read_events(destination))
    return ExtractResult(
        extract_path=str(destination),
        source=SOURCE_API,
        row_count=row_count,
        window_start=window_start,
        window_end=window_end,
        overwrite_window=True,
        min_magnitude=settings.min_magnitude,
    )


def extract_csv_file(
    settings: Settings,
    source: Path,
    window_start: datetime,
    window_end: datetime,
    *,
    overwrite_window: bool,
) -> ExtractResult:
    """Load a local CSV, optionally restricted to a half-open event-time window."""

    window_start = naive_utc(window_start)
    window_end = naive_utc(window_end)
    _require_window(window_start, window_end)
    events = read_events(source)
    filtered = filter_events(
        events,
        min_magnitude=settings.min_magnitude,
        window_start=window_start if overwrite_window else None,
        window_end=window_end if overwrite_window else None,
    )
    destination = _destination(settings, SOURCE_FILE, window_start, window_end)
    write_events(destination, filtered)
    return ExtractResult(
        extract_path=str(destination),
        source=SOURCE_FILE,
        row_count=len(filtered),
        window_start=window_start,
        window_end=window_end,
        overwrite_window=overwrite_window,
        min_magnitude=settings.min_magnitude,
    )


def download_usgs_csv(url: str, timeout: int) -> bytes:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "text/csv"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        raise ExtractError(f"USGS request failed with HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise ExtractError(f"USGS request failed: {exc.reason}") from exc
    return payload


def looks_like_event_csv(payload: bytes) -> bool:
    text = payload.lstrip(b"\xef\xbb\xbf").lstrip().lower()
    return text.startswith(b"time,")


def _materialize_sample(
    settings: Settings,
    destination: Path,
    window_start: datetime,
    window_end: datetime,
) -> ExtractResult:
    if not settings.sample_csv.is_file():
        raise ExtractError(f"bundled sample CSV is missing: {settings.sample_csv}")
    events = filter_events(read_events(settings.sample_csv), min_magnitude=settings.min_magnitude)
    write_events(destination, events)
    return ExtractResult(
        extract_path=str(destination),
        source=SOURCE_SAMPLE,
        row_count=len(events),
        window_start=window_start,
        window_end=window_end,
        overwrite_window=False,
        min_magnitude=settings.min_magnitude,
    )


def _destination(
    settings: Settings, kind: str, window_start: datetime, window_end: datetime
) -> Path:
    settings.extract_dir.mkdir(parents=True, exist_ok=True)
    stamp = f"{window_start:%Y%m%dT%H%M%S}_{window_end:%Y%m%dT%H%M%S}"
    return settings.extract_dir / f"earthquakes_{kind}_{stamp}.csv"


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _require_window(window_start: datetime, window_end: datetime) -> None:
    if window_start >= window_end:
        raise ExtractError("window_start must be earlier than window_end")


def _stamp(value: datetime) -> str:
    return naive_utc(value).isoformat(timespec="seconds")


def _require_timestamp(value: object) -> datetime:
    parsed = parse_timestamp(str(value))
    if parsed is None:
        raise ExtractError(f"invalid extract timestamp: {value!r}")
    return parsed


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
