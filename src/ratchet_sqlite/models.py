"""Ratchet-private records.

The cross-component commit objects live in :mod:`ratchet_sqlite.contracts`. These
records expose Ratchet status without leaking SQLite rows or inventing another
cross-component schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .contracts import CheckpointTier, require_run_id


@dataclass(frozen=True, slots=True)
class Lease:
    instance_id: str
    owner_id: str
    lease_id: str
    fencing_token: int
    acquired_at: str
    expires_at: str


@dataclass(frozen=True, slots=True)
class AttemptHandle:
    instance_id: str
    run_id: str
    attempt_id: str
    fencing_token: int
    plan_digest: str
    effective_config_digest: str
    runtime_lock_digest: str
    requested_tier: CheckpointTier
    minimum_tier: CheckpointTier
    effective_tier: CheckpointTier
    started_at: str

    def __post_init__(self) -> None:
        require_run_id(self.run_id)


@dataclass(frozen=True, slots=True)
class AttemptSummary:
    instance_id: str
    run_id: str
    attempt_id: str
    status: str
    fencing_token: int
    effective_tier: CheckpointTier
    started_at: str
    updated_at: str
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class CheckpointRecord:
    instance_id: str
    attempt_id: str
    boundary_tier: CheckpointTier
    boundary_id: str
    proof_id: str
    proof_digest: str
    recorded_at: str


@dataclass(frozen=True, slots=True)
class CommittedPosition:
    source_id: str
    position_type: str
    encoded_value: str
    coverage_digest: str
    certificate_id: str
    committed_at: str


EffectState = Literal["prepared", "dispatching", "ambiguous", "verified", "cancelled"]
RecoveryAction = Literal["safe_to_dispatch", "readback_required", "no_action"]


@dataclass(frozen=True, slots=True)
class EffectIntent:
    instance_id: str
    attempt_id: str
    effect_id: str
    provider: str
    capability: str
    operation_digest: str
    idempotency_key: str
    state: EffectState
    prepared_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class EffectRecovery:
    effect: EffectIntent
    action: RecoveryAction
    reason: str | None


@dataclass(frozen=True, slots=True)
class ActiveSnapshot:
    instance_id: str
    snapshot_digest: str
    certificate_id: str
    activated_at: str


@dataclass(frozen=True, slots=True)
class RecoveryStatus:
    instance_id: str
    active_snapshot: ActiveSnapshot | None
    attempts: tuple[AttemptSummary, ...]
    effects: tuple[EffectRecovery, ...]


@dataclass(frozen=True, slots=True)
class InstanceStatus:
    instance_id: str
    current_lease: Lease | None
    active_snapshot: ActiveSnapshot | None
    committed_positions: tuple[CommittedPosition, ...]
    attempts: tuple[AttemptSummary, ...]
    unresolved_effects: tuple[EffectRecovery, ...]
