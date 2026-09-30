"""Small validation primitives shared by contract dataclasses."""

from __future__ import annotations

import re
from collections.abc import Iterable

_ID = re.compile(r"^[a-z][a-z0-9_.:-]*$")
RUN_ID_PATTERN = r"^[a-z][a-z0-9_.:-]*$"
_RUN_ID = re.compile(RUN_ID_PATTERN)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class RunIdValidationError(ValueError):
    """Stable, cross-package failure for a non-canonical run identity."""

    code = "run_id.invalid"


def require_run_id(value: str, field: str = "run_id") -> str:
    """Return one already-canonical run id or fail without transforming it."""

    if not isinstance(value, str) or not _RUN_ID.fullmatch(value):
        raise RunIdValidationError(
            f"{field} must match {RUN_ID_PATTERN!r}; uppercase and whitespace are not canonical"
        )
    return value


def require_id(value: str, field: str) -> None:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ValueError(f"{field} must match {_ID.pattern!r}")


def require_digest(value: str, field: str) -> None:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase sha256 digest")


def require_nonempty(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty")


def require_unique(values: Iterable[str], field: str) -> None:
    materialized = tuple(values)
    if len(materialized) != len(set(materialized)):
        raise ValueError(f"{field} must contain unique values")
