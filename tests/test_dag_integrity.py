from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_ORDER = [
    "extract",
    "load_raw",
    "build_staging",
    "build_marts",
    "run_quality_checks",
]


@pytest.fixture(scope="module")
def dag():
    pytest.importorskip("airflow")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert list(bag.dags) == ["usgs_earthquakes_elt"]
    # bag.dags is the parsed DAG. DagBag.get_dag also reads the metadata DB.
    return bag.dags["usgs_earthquakes_elt"]


def test_dag_imports_and_has_no_cycles(dag):
    if hasattr(dag, "check_cycle"):
        dag.check_cycle()
    ordered = [task.task_id for task in dag.topological_sort()]
    assert ordered == EXPECTED_ORDER
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(hours=2)
    summary = str(dag.timetable.summary)
    assert summary in {"@daily", "0 0 * * *"}, summary


def test_retries_sla_and_timeouts(dag):
    assert dag.default_args["retries"] == 2
    assert dag.default_args["sla"] == timedelta(hours=1)
    extract = dag.get_task("extract")
    load = dag.get_task("load_raw")
    quality = dag.get_task("run_quality_checks")
    assert extract.retries == 2
    assert load.retries == 2
    assert extract.retry_delay == timedelta(minutes=5)
    assert extract.sla == timedelta(hours=1)
    assert load.sla == timedelta(hours=1)
    assert quality.sla == timedelta(hours=1)
    assert quality.retries == 0
    assert extract.execution_timeout == timedelta(minutes=30)
    assert dag.get_task("load_raw").downstream_task_ids == {"build_staging"}


def test_failure_and_sla_callbacks_can_be_called(dag):
    failure = dag.default_args["on_failure_callback"]
    failure(
        {
            "dag": dag,
            "task_instance": None,
            "run_id": "manual__2024-01-01",
            "exception": RuntimeError("boom"),
        }
    )
    callback = dag.sla_miss_callback
    callbacks = callback if isinstance(callback, list) else [callback]
    assert callbacks[0] is not None
    callbacks[0](dag, [dag.get_task("extract")], [], [], [])


def test_task_lineage_points_raw_staging_and_marts(dag):
    assert not dag.get_task("extract").outlets
    assert dag.get_task("load_raw").outlets
    assert dag.get_task("build_staging").inlets
    assert dag.get_task("build_staging").outlets
    assert dag.get_task("build_marts").inlets
    assert dag.get_task("build_marts").outlets
    assert dag.get_task("run_quality_checks").inlets
