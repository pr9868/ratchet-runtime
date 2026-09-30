"""Deterministic, domain-neutral monotonic position comparators."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from .errors import UnknownComparator

Comparator = Callable[[str, str], int]


def _ordered(left: object, right: object) -> int:
    return (left > right) - (left < right)  # type: ignore[operator]


def integer_comparator(left: str, right: str) -> int:
    try:
        return _ordered(int(left), int(right))
    except ValueError as exc:
        raise ValueError("integer positions must be base-10 integers") from exc


def decimal_comparator(left: str, right: str) -> int:
    try:
        left_value = Decimal(left)
        right_value = Decimal(right)
    except InvalidOperation as exc:
        raise ValueError("decimal positions must be finite decimal strings") from exc
    if not left_value.is_finite() or not right_value.is_finite():
        raise ValueError("decimal positions must be finite decimal strings")
    return _ordered(left_value, right_value)


def lexical_comparator(left: str, right: str) -> int:
    return _ordered(left, right)


def iso8601_comparator(left: str, right: str) -> int:
    def parse(value: str) -> datetime:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            raise ValueError("ISO-8601 positions must include a timezone")
        return parsed.astimezone(timezone.utc)

    return _ordered(parse(left), parse(right))


def integer_tuple_comparator(left: str, right: str) -> int:
    def parse(value: str) -> tuple[int, ...]:
        decoded = json.loads(value)
        if not isinstance(decoded, list) or not decoded or any(
            isinstance(item, bool) or not isinstance(item, int) for item in decoded
        ):
            raise ValueError("integer-tuple positions must be a non-empty JSON integer array")
        return tuple(decoded)

    return _ordered(parse(left), parse(right))


def equality_only_comparator(left: str, right: str) -> int:
    if left != right:
        raise ValueError("opaque positions may be repeated but cannot advance without a comparator")
    return 0


class ComparatorRegistry:
    """Explicit registry; adapters may add a comparator without teaching Ratchet semantics."""

    def __init__(self) -> None:
        self._comparators: dict[str, Comparator] = {
            "integer": integer_comparator,
            "sequence": integer_comparator,
            "decimal": decimal_comparator,
            "lexical": lexical_comparator,
            "iso8601": iso8601_comparator,
            "integer-tuple": integer_tuple_comparator,
            "opaque-equality": equality_only_comparator,
        }

    def register(self, position_type: str, comparator: Comparator) -> None:
        if not position_type or not callable(comparator):
            raise ValueError("a non-empty position type and callable comparator are required")
        self._comparators[position_type] = comparator

    def compare(self, position_type: str, left: str, right: str) -> int:
        try:
            comparator = self._comparators[position_type]
        except KeyError as exc:
            raise UnknownComparator(f"no comparator registered for {position_type!r}") from exc
        return comparator(left, right)

    def knows(self, position_type: str) -> bool:
        return position_type in self._comparators
