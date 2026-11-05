"""Local runner. The default is the bundled sample, so the demo does not need network or Airflow."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeline.config import SAMPLE_WINDOW_END, SAMPLE_WINDOW_START, Settings, load_settings
from pipeline.errors import DataQualityError, PipelineError
from pipeline.records import naive_utc, parse_timestamp
from pipeline.reporting import format_report
from pipeline.service import run_elt


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        settings = _settings_from_args(args)
        window_start, window_end = _window_from_args(args)
        report = run_elt(
            settings,
            window_start,
            window_end,
            source_csv=args.from_csv,
        )
    except DataQualityError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except PipelineError as exc:
        print(f"pipeline failed: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"pipeline failed: {exc}", file=sys.stderr)
        return 2
    print(format_report(report), end="")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description="Run the USGS earthquake ELT without the Airflow scheduler.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser(
        "run",
        help="Extract, load, transform, and run data quality checks",
    )
    mode = run.add_mutually_exclusive_group()
    mode.add_argument(
        "--offline",
        action="store_true",
        help="Replay the bundled January 2024 sample. This is the default.",
    )
    mode.add_argument(
        "--online",
        action="store_true",
        help="Download the USGS window. Falls back to the sample unless disabled.",
    )
    mode.add_argument(
        "--from-csv",
        type=Path,
        help="Load this CSV and overwrite the half-open --start/--end window.",
    )
    run.add_argument("--start", help="Window start, YYYY-MM-DD or ISO-8601. Inclusive.")
    run.add_argument("--end", help="Window end, YYYY-MM-DD or ISO-8601. Exclusive.")
    run.add_argument(
        "--duckdb-path", type=Path, help="DuckDB file. Ignored when --warehouse-url is set."
    )
    run.add_argument(
        "--warehouse-url",
        help="Postgres SQLAlchemy-style URL. When set, DuckDB is not used.",
    )
    run.add_argument(
        "--extract-dir", type=Path, help="Directory for the extracted CSV of this run."
    )
    run.add_argument(
        "--min-magnitude", type=float, help="Magnitude floor for local files and the USGS query."
    )
    run.add_argument(
        "--no-sample-fallback",
        action="store_true",
        help="With --online, fail the run when USGS cannot be reached.",
    )
    return parser


def _settings_from_args(args: argparse.Namespace) -> Settings:
    settings = load_settings()
    overrides: dict[str, object] = {}
    if args.from_csv is not None:
        overrides["offline"] = False
    elif args.online:
        overrides["offline"] = False
    else:
        overrides["offline"] = True
    if args.no_sample_fallback:
        overrides["allow_sample_fallback"] = False
    if args.duckdb_path is not None:
        overrides["duckdb_path"] = args.duckdb_path
    if args.warehouse_url is not None:
        overrides["warehouse_url"] = args.warehouse_url.strip() or None
    if args.extract_dir is not None:
        overrides["extract_dir"] = args.extract_dir
    if args.min_magnitude is not None:
        overrides["min_magnitude"] = args.min_magnitude
    return settings.with_overrides(**overrides)


def _window_from_args(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.from_csv is not None and (not args.start or not args.end):
        raise ValueError("--from-csv requires both --start and --end")
    if bool(args.start) ^ bool(args.end):
        raise ValueError("provide both --start and --end (end is exclusive)")
    if args.start and args.end:
        window_start = _parse_when(args.start, "--start")
        window_end = _parse_when(args.end, "--end")
    elif args.online:
        window_start, window_end = _trailing_utc_day(datetime.now(UTC))
    else:
        window_start, window_end = SAMPLE_WINDOW_START, SAMPLE_WINDOW_END
    if window_start >= window_end:
        raise ValueError("--start must be earlier than --end")
    return window_start, window_end


def _parse_when(value: str, label: str) -> datetime:
    if len(value) == 10:
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"{label} must be YYYY-MM-DD or ISO-8601") from exc
    parsed = parse_timestamp(value)
    if parsed is None:
        raise ValueError(f"{label} must be YYYY-MM-DD or ISO-8601")
    return parsed


def _trailing_utc_day(now: datetime) -> tuple[datetime, datetime]:
    today = naive_utc(now).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=1), today


if __name__ == "__main__":
    sys.exit(main())
