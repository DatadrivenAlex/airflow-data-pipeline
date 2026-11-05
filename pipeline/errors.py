"""Pipeline exceptions. Data-quality failures keep the failed check details."""

from __future__ import annotations

from collections.abc import Sequence


class PipelineError(Exception):
    """Raised when the pipeline cannot complete because of configuration or I/O."""


class ExtractError(PipelineError):
    """Raised when a USGS download or local extract cannot be produced."""


class LoadError(PipelineError):
    """Raised when raw rows cannot be written to the warehouse."""


class TransformError(PipelineError):
    """Raised when a staging or mart SQL model cannot be applied."""


class DataQualityError(PipelineError):
    """Raised after load when one or more data-quality checks fail.

    The warehouse commit is already done, so the failing rows can be queried.
    """

    def __init__(self, failures: Sequence[object]):
        self.failures = list(failures)
        lines = []
        for item in self.failures:
            check_id = getattr(item, "check_id", "check")
            detail = getattr(item, "detail", str(item))
            lines.append(f"- {check_id}: {detail}")
        message = "data quality checks failed"
        if lines:
            message = f"{message}\n" + "\n".join(lines)
        super().__init__(message)
