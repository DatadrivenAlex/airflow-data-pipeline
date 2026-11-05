"""Independent aggregation of a USGS CSV, used to check the SQL models."""

from __future__ import annotations

import csv
from pathlib import Path


def region_name(place: str | None) -> str:
    if place is None or not place.strip():
        return "Unknown"
    if "," in place:
        tail = place.split(",", 1)[1].strip()
        if tail:
            return tail
    return place.strip()


def catalog_rows(path: Path, *, min_magnitude: float | None = None) -> list[dict[str, object]]:
    """Keep the latest revision of each earthquake id.

    Timestamp strings from USGS sort lexicographically, so the comparison does
    not go through the pipeline parser.
    """

    with path.open(newline="", encoding="utf-8-sig") as handle:
        raw_rows = list(csv.DictReader(handle))

    best: dict[str, tuple[str, int, dict[str, str]]] = {}
    for index, row in enumerate(raw_rows):
        event_id = (row.get("id") or "").strip()
        if not event_id:
            continue
        updated = (row.get("updated") or "").strip()
        current = best.get(event_id)
        if current is None or (updated, index) >= (current[0], current[1]):
            best[event_id] = (updated, index, row)

    selected: list[dict[str, object]] = []
    for _updated, _index, row in best.values():
        if (row.get("type") or "").strip() != "earthquake":
            continue
        mag_text = (row.get("mag") or "").strip()
        magnitude = float(mag_text) if mag_text else None
        if min_magnitude is not None and magnitude is not None and magnitude < min_magnitude:
            continue
        selected.append(
            {
                "id": (row.get("id") or "").strip(),
                "date": (row.get("time") or "")[:10],
                "magnitude": magnitude,
                "region": region_name(row.get("place")),
            }
        )
    return selected


def daily_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        day = str(row["date"])
        counts[day] = counts.get(day, 0) + 1
    return counts


def region_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        region = str(row["region"])
        counts[region] = counts.get(region, 0) + 1
    return counts
