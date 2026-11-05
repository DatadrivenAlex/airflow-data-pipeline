"""Shared ELT steps for the CLI and the Airflow tasks."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pipeline.config import Settings
from pipeline.extract import Downloader, ExtractResult, extract_csv_file, extract_step
from pipeline.load import load_extract
from pipeline.quality import enforce
from pipeline.reporting import RunReport, collect_report
from pipeline.transform import apply_layer


def run_elt(
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
    *,
    source_csv: Path | None = None,
    now: datetime | None = None,
    downloader: Downloader | None = None,
) -> RunReport:
    if source_csv is not None:
        extracted = extract_csv_file(
            settings,
            source_csv,
            window_start,
            window_end,
            overwrite_window=True,
        )
    else:
        extracted = extract_step(
            settings,
            window_start,
            window_end,
            downloader=downloader,
        )
    return run_from_extract(settings, extracted, now=now)


def run_from_extract(
    settings: Settings,
    extracted: ExtractResult,
    *,
    now: datetime | None = None,
) -> RunReport:
    loaded = load_extract(settings, extracted, now=now)
    apply_layer(settings, "staging")
    apply_layer(settings, "marts")
    checks = enforce(settings, loaded.load_id, now=now)
    return collect_report(settings, loaded, checks)
