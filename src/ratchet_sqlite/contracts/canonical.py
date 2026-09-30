"""Deterministic serialization and digest helpers.

YAML may be used for human configuration, but identity is always computed over the parsed canonical
JSON value. Timestamps remain explicit strings so callers cannot accidentally discard offsets.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


def to_primitive(value: Any) -> Any:
    """Convert supported contract values to a JSON-compatible, deterministic value."""

    if dataclasses.is_dataclass(value):
        return {
            field.name: to_primitive(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical mapping keys must be strings")
        return {key: to_primitive(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_primitive(item) for item in value]
    if isinstance(value, (set, frozenset)):
        primitive = [to_primitive(item) for item in value]
        return sorted(primitive, key=lambda item: canonical_json(item))
    if isinstance(value, Path):
        return value.as_posix()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Return RFC-8259-compatible canonical JSON for contract identity."""

    return json.dumps(
        to_primitive(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_digest(value: Any) -> str:
    """Return a lowercase sha256 digest of the canonical JSON representation."""

    encoded = canonical_json(value).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def digest_identifier(prefix: str, value: Any) -> str:
    """Derive a contract-valid framework identifier from a canonical digest.

    Contract identifiers must match ``^[a-z][a-z0-9_.:-]*$``. A bare sha256
    digest starts with a digit for ten of sixteen possible first characters, so
    using one directly as an identifier fails validation for roughly 62% of
    inputs. The semantic `prefix` guarantees a leading letter and additionally
    keeps framework-created identifiers self-describing and distinguishable
    from provider-controlled external identifiers (V2-PROVIDER-003).
    """

    if not prefix or not prefix[0].isalpha() or not prefix.replace("-", "").replace("_", "").isalnum():
        raise ValueError(f"identifier prefix must start with a letter and be alphanumeric: {prefix!r}")
    return f"{prefix.casefold()}-{canonical_digest(value)}"
