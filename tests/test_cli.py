from pipeline.cli import main
from tests.conftest import feed_row, write_feed


def test_cli_offline_run_prints_a_passing_report(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("WAREHOUSE_URL", raising=False)
    code = main(
        [
            "run",
            "--offline",
            "--duckdb-path",
            str(tmp_path / "warehouse.duckdb"),
            "--extract-dir",
            str(tmp_path / "extracts"),
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    assert "run_source: sample" in captured.out
    assert "quality: pass" in captured.out
    assert "audit_status: success" in captured.out
    assert "mart_daily_seismicity" in captured.out


def test_cli_rejects_an_inverted_window(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("WAREHOUSE_URL", raising=False)
    code = main(
        [
            "run",
            "--offline",
            "--start",
            "2024-02-01",
            "--end",
            "2024-01-01",
            "--duckdb-path",
            str(tmp_path / "warehouse.duckdb"),
            "--extract-dir",
            str(tmp_path / "extracts"),
        ]
    )
    assert code == 2
    assert "earlier" in capsys.readouterr().err


def test_cli_fails_when_quality_checks_fail(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("WAREHOUSE_URL", raising=False)
    path = tmp_path / "quarry.csv"
    write_feed(
        path,
        [
            feed_row(
                "q1",
                "2024-01-01T00:30:00Z",
                "4.8",
                "10 km S of X, Japan",
                event_type="quarry blast",
            )
        ],
    )
    code = main(
        [
            "run",
            "--from-csv",
            str(path),
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--min-magnitude",
            "0",
            "--duckdb-path",
            str(tmp_path / "warehouse.duckdb"),
            "--extract-dir",
            str(tmp_path / "extracts"),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    assert "data quality checks failed" in captured.err
    assert "staging_min_rows" in captured.err
