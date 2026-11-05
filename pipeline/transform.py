"""Run staging and mart SQL. Each file is one SELECT, materialized as a table."""

from __future__ import annotations

import re
from pathlib import Path

from pipeline.config import Settings
from pipeline.errors import TransformError
from pipeline.warehouse import ensure_storage, open_warehouse

TRANSFORMS_DIR = Path(__file__).resolve().parent / "transforms"
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
_COMMENT = re.compile(r"--.*?$", re.MULTILINE)


def apply_layer(settings: Settings, layer: str) -> list[str]:
    if layer not in {"staging", "marts"}:
        raise TransformError(f"unknown transform layer: {layer}")
    directory = TRANSFORMS_DIR / layer
    paths = sorted(directory.glob("*.sql"))
    if not paths:
        raise TransformError(f"no SQL models in {directory}")

    warehouse = open_warehouse(settings)
    applied: list[str] = []
    try:
        warehouse.begin()
        ensure_storage(warehouse)
        for path in paths:
            table = path.stem
            if not _IDENT.match(table):
                raise TransformError(f"invalid model name: {path.name}")
            select_sql = _read_select(path)
            qualified = f"{layer}.{table}"
            warehouse.execute(f"DROP TABLE IF EXISTS {qualified}")
            warehouse.execute(f"CREATE TABLE {qualified} AS\n{select_sql}")
            applied.append(qualified)
        warehouse.commit()
    except Exception:
        warehouse.rollback()
        raise
    finally:
        warehouse.close()
    return applied


def _read_select(path: Path) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    if ";" in raw:
        raise TransformError(f"{path.name} must be a single SELECT and cannot contain ';'")
    stripped = _COMMENT.sub("", raw).strip()
    if not stripped.lower().startswith(("select", "with")):
        raise TransformError(f"{path.name} must start with SELECT or WITH")
    return raw
