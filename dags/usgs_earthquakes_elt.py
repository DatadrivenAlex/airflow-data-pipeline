"""Daily USGS earthquake ELT.

The DAG extracts one data interval from the public USGS FDSN event feed, loads
raw rows into DuckDB (or Postgres when ``WAREHOUSE_URL`` is set), rebuilds the
staging and mart tables from SQL, and fails the run if data-quality checks fail.

``ELT_OFFLINE=1`` replays the bundled January 2024 sample instead of calling
USGS. That sample is also the fallback when the feed cannot be reached, unless
``ELT_ALLOW_SAMPLE_FALLBACK=0``. Fallback does not delete the requested
partition, so a live outage cannot wipe a day that already loaded.
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from airflow.datasets import Dataset  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402

from pipeline.config import load_settings  # noqa: E402
from pipeline.extract import ExtractResult, extract_step  # noqa: E402
from pipeline.load import LoadResult, load_extract  # noqa: E402
from pipeline.quality import enforce  # noqa: E402
from pipeline.reporting import collect_report, format_report  # noqa: E402
from pipeline.transform import apply_layer  # noqa: E402

logger = logging.getLogger(__name__)

RAW_EVENTS = Dataset("warehouse://raw/earthquakes")
STAGING_EVENTS = Dataset("warehouse://staging/stg_earthquakes")
MART_EVENTS = Dataset("warehouse://marts/earthquakes")

DAG_ID = "usgs_earthquakes_elt"


def log_task_failure(context: dict) -> None:
    """Log a failed task. Airflow also retries extract and load on their own."""

    task_instance = context.get("task_instance")
    dag_obj = context.get("dag")
    message = "task failed dag=%s task=%s run_id=%s error=%s"
    args = (
        getattr(dag_obj, "dag_id", "unknown"),
        getattr(task_instance, "task_id", "unknown"),
        context.get("run_id", "unknown"),
        context.get("exception"),
    )
    task_log = getattr(task_instance, "log", None)
    if task_log is not None:
        task_log.error(message, *args)
    else:
        logger.error(message, *args)


def log_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Record an SLA miss. In Airflow 2 this callback does not fail the task."""

    del slas, blocking_tis
    logger.error(
        "SLA miss dag=%s tasks=%s blocking_tasks=%s",
        getattr(dag, "dag_id", dag),
        [getattr(task, "task_id", str(task)) for task in task_list],
        [getattr(task, "task_id", str(task)) for task in blocking_task_list],
    )


default_args = {
    "owner": "Alexandru Cojocari",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=1),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": log_task_failure,
}


@dag(
    dag_id=DAG_ID,
    description="Load USGS earthquakes, build staging and marts, and fail on quality checks.",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    sla_miss_callback=log_sla_miss,
    tags=["elt", "usgs", "duckdb"],
    doc_md=__doc__,
)
def usgs_earthquakes_elt():
    @task
    def extract(data_interval_start: datetime, data_interval_end: datetime) -> dict:
        """Download one USGS interval, or replay the bundled sample."""

        result = extract_step(load_settings(), data_interval_start, data_interval_end)
        return result.to_dict()

    @task(outlets=[RAW_EVENTS])
    def load_raw(extract_payload: dict) -> dict:
        """Upsert raw rows by event id. API runs also replace the time partition."""

        loaded = load_extract(load_settings(), ExtractResult.from_dict(extract_payload))
        return loaded.to_dict()

    @task(inlets=[RAW_EVENTS], outlets=[STAGING_EVENTS])
    def build_staging(load_payload: dict) -> dict:
        """Rebuild staging.stg_earthquakes from the raw table."""

        apply_layer(load_settings(), "staging")
        return load_payload

    @task(inlets=[STAGING_EVENTS], outlets=[MART_EVENTS])
    def build_marts(load_payload: dict) -> dict:
        """Rebuild the daily, region, and significant-event marts."""

        apply_layer(load_settings(), "marts")
        return load_payload

    @task(inlets=[MART_EVENTS], retries=0)
    def run_quality_checks(load_payload: dict) -> dict:
        """Fail the DAG when row, null, uniqueness, freshness, or mart checks fail.

        Quality is not retried: the same warehouse state would fail again.
        Extract and load keep the DAG retries. The failed rows stay committed
        so they can be inspected before a clear-and-rerun.
        """

        settings = load_settings()
        loaded = LoadResult.from_dict(load_payload)
        outcomes = enforce(settings, loaded.load_id)
        report = collect_report(settings, loaded, outcomes)
        logger.info("\n%s", format_report(report))
        return {
            "load_id": loaded.load_id,
            "quality": "pass",
            "checks": len(outcomes),
        }

    run_quality_checks(build_marts(build_staging(load_raw(extract()))))


usgs_earthquakes_elt()
