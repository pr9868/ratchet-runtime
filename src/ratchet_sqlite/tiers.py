"""Checkpoint tier ordering and escalation."""

from __future__ import annotations

from .contracts import CheckpointTier


_TIER_ORDER = {
    CheckpointTier.RUN: 0,
    CheckpointTier.PHASE: 1,
    CheckpointTier.NODE: 2,
    CheckpointTier.FORENSIC: 3,
}


def tier_rank(tier: CheckpointTier) -> int:
    try:
        return _TIER_ORDER[tier]
    except KeyError as exc:
        raise ValueError(f"unsupported checkpoint tier: {tier!r}") from exc


def effective_checkpoint_tier(
    requested: CheckpointTier, minimum: CheckpointTier
) -> CheckpointTier:
    """Return the stricter tier; a caller cannot lower a policy minimum."""

    return requested if tier_rank(requested) >= tier_rank(minimum) else minimum


def checkpoint_is_enabled(effective: CheckpointTier, boundary: CheckpointTier) -> bool:
    """Whether an effective tier persists a boundary of the supplied granularity."""

    return tier_rank(boundary) <= tier_rank(effective)
