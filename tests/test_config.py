import pytest

from pipeline.config import DEFAULT_MIN_MAGNITUDE, DEFAULT_SAMPLE_CSV, load_settings
from pipeline.errors import PipelineError


def test_defaults_try_the_live_feed(monkeypatch):
    for name in (
        "ELT_OFFLINE",
        "ELT_ALLOW_SAMPLE_FALLBACK",
        "DUCKDB_PATH",
        "WAREHOUSE_URL",
        "SAMPLE_CSV_PATH",
        "EXTRACT_DIR",
        "USGS_MIN_MAGNITUDE",
        "USGS_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = load_settings()
    assert settings.offline is False
    assert settings.allow_sample_fallback is True
    assert settings.warehouse_url is None
    assert settings.min_magnitude == DEFAULT_MIN_MAGNITUDE
    assert settings.sample_csv == DEFAULT_SAMPLE_CSV
    assert settings.request_timeout_seconds == 60


def test_blank_warehouse_url_keeps_duckdb(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_URL", "   ")
    assert load_settings().warehouse_url is None


def test_invalid_flag_is_rejected(monkeypatch):
    monkeypatch.setenv("ELT_OFFLINE", "sometimes")
    with pytest.raises(PipelineError, match="ELT_OFFLINE"):
        load_settings()
