"""Parse the USGS event CSV. Raw text is preserved and timestamps are UTC."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipeline.errors import LoadError

CSV_COLUMNS = [
    "time",
    "latitude",
    "longitude",
    "depth",
    "mag",
    "magType",
    "nst",
    "gap",
    "dmin",
    "rms",
    "net",
    "id",
    "updated",
    "place",
    "type",
    "horizontalError",
    "depthError",
    "magError",
    "magNst",
    "status",
    "locationSource",
    "magSource",
]

REQUIRED_CSV_COLUMNS = {
    "time",
    "latitude",
    "longitude",
    "depth",
    "mag",
    "id",
    "updated",
    "place",
    "type",
}


@dataclass(frozen=True)
class SourceEvent:
    time_text: str
    latitude_text: str
    longitude_text: str
    depth_text: str
    mag_text: str
    mag_type_text: str
    nst_text: str
    gap_text: str
    dmin_text: str
    rms_text: str
    net_text: str
    event_id: str | None
    updated_text: str
    place_text: str
    event_type_text: str
    horizontal_error_text: str
    depth_error_text: str
    mag_error_text: str
    mag_nst_text: str
    status_text: str
    location_source_text: str
    mag_source_text: str
    event_time: datetime | None
    updated_at: datetime | None

    def as_csv_row(self) -> dict[str, str]:
        return {
            "time": self.time_text,
            "latitude": self.latitude_text,
            "longitude": self.longitude_text,
            "depth": self.depth_text,
            "mag": self.mag_text,
            "magType": self.mag_type_text,
            "nst": self.nst_text,
            "gap": self.gap_text,
            "dmin": self.dmin_text,
            "rms": self.rms_text,
            "net": self.net_text,
            "id": self.event_id or "",
            "updated": self.updated_text,
            "place": self.place_text,
            "type": self.event_type_text,
            "horizontalError": self.horizontal_error_text,
            "depthError": self.depth_error_text,
            "magError": self.mag_error_text,
            "magNst": self.mag_nst_text,
            "status": self.status_text,
            "locationSource": self.location_source_text,
            "magSource": self.mag_source_text,
        }


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse a USGS timestamp into naive UTC. Blank or garbage values return None."""

    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return naive_utc(parsed)


def naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def read_events(path: Path) -> list[SourceEvent]:
    path = Path(path)
    if not path.is_file():
        raise LoadError(f"extract file does not exist: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = REQUIRED_CSV_COLUMNS.difference(fieldnames)
        if missing:
            ordered = ", ".join(sorted(missing))
            raise LoadError(f"{path} is missing required USGS columns: {ordered}")
        return [event_from_mapping(row) for row in reader]


def write_events(path: Path, events: list[SourceEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        for event in events:
            writer.writerow(event.as_csv_row())


def filter_events(
    events: list[SourceEvent],
    *,
    min_magnitude: float | None = None,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> list[SourceEvent]:
    """Apply a half-open event-time window and an optional magnitude floor.

    Rows whose magnitude cannot be parsed are kept so a later null check can
    fail the DAG. Rows whose timestamp cannot be parsed are dropped only when
    a window is set, because they cannot belong to that partition.
    """

    selected: list[SourceEvent] = []
    for event in events:
        if min_magnitude is not None:
            magnitude = parse_float(event.mag_text)
            if magnitude is not None and magnitude < min_magnitude:
                continue
        if window_start is not None or window_end is not None:
            if event.event_time is None:
                continue
            if window_start is not None and event.event_time < window_start:
                continue
            if window_end is not None and event.event_time >= window_end:
                continue
        selected.append(event)
    return selected


def collapse_latest(events: list[SourceEvent]) -> list[SourceEvent]:
    """Keep the newest revision of each event id. Blank ids are all retained."""

    blanks: list[SourceEvent] = []
    best: dict[str, tuple[int, SourceEvent]] = {}
    for index, event in enumerate(events):
        if not event.event_id:
            blanks.append(event)
            continue
        current = best.get(event.event_id)
        if current is None or _outranks(event, index, current[1], current[0]):
            best[event.event_id] = (index, event)
    return blanks + [item[1] for item in best.values()]


def event_from_mapping(row: dict[str, str | None]) -> SourceEvent:
    def cell(name: str) -> str:
        value = row.get(name)
        if value is None:
            return ""
        return str(value).strip()

    event_id = cell("id") or None
    time_text = cell("time")
    updated_text = cell("updated")
    return SourceEvent(
        time_text=time_text,
        latitude_text=cell("latitude"),
        longitude_text=cell("longitude"),
        depth_text=cell("depth"),
        mag_text=cell("mag"),
        mag_type_text=cell("magType"),
        nst_text=cell("nst"),
        gap_text=cell("gap"),
        dmin_text=cell("dmin"),
        rms_text=cell("rms"),
        net_text=cell("net"),
        event_id=event_id,
        updated_text=updated_text,
        place_text=cell("place"),
        event_type_text=cell("type"),
        horizontal_error_text=cell("horizontalError"),
        depth_error_text=cell("depthError"),
        mag_error_text=cell("magError"),
        mag_nst_text=cell("magNst"),
        status_text=cell("status"),
        location_source_text=cell("locationSource"),
        mag_source_text=cell("magSource"),
        event_time=parse_timestamp(time_text),
        updated_at=parse_timestamp(updated_text),
    )


def _outranks(
    candidate: SourceEvent,
    candidate_index: int,
    current: SourceEvent,
    current_index: int,
) -> bool:
    if candidate.updated_at != current.updated_at:
        if candidate.updated_at is None:
            return False
        if current.updated_at is None:
            return True
        return candidate.updated_at > current.updated_at
    return candidate_index > current_index
