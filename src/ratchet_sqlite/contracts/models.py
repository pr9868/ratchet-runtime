"""Public, domain-neutral execution values. No application or provider dependencies."""
from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from .canonical import canonical_digest
from .validation import require_digest, require_id, require_nonempty, require_run_id, require_unique
CONTRACT_VERSION = "1.0"

class CheckpointTier(StrEnum):
    RUN = "run"
    PHASE = "phase"
    NODE = "node"
    FORENSIC = "forensic"


class NodeKind(StrEnum):
    PURE = "pure"
    EVIDENCE_READ = "evidence_read"
    LOCAL_DURABLE_WRITE = "local_durable_write"
    EXTERNAL_READ = "external_read"
    EXTERNAL_EFFECT = "external_effect"
    HUMAN_GATE = "human_gate"
    VERIFICATION = "verification"


NON_WAIVABLE_REASON_CODES: frozenset[str] = frozenset(
    {
        "cursor_atomicity",
        "fencing_failure",
        "unverified_external_write",
        "missing_effect_receipt",
        "commit_certificate_failure",
        "source_coverage_unknown",
        "credential_or_scope_escalation",
    }
)


class VerificationStatus(StrEnum):
    VERIFIED = "verified"
    FAILED = "failed"
    DEGRADED = "degraded"


class TerminalOutcome(StrEnum):
    COMPLETED = "completed"
    COMPLETED_WITH_EXCEPTIONS = "completed_with_exceptions"
    NO_CHANGE = "no_change"
    GATED = "gated"
    DEGRADED_UNCOMMITTED = "degraded_uncommitted"
    FAILED_RECOVERABLE = "failed_recoverable"
    FAILED_TERMINAL = "failed_terminal"
    ABANDONED = "abandoned"


@dataclass(frozen=True, slots=True)
class ProposedPosition:
    source_id: str
    position_type: str
    encoded_value: str
    coverage_digest: str

    def __post_init__(self) -> None:
        require_id(self.source_id, "source_id")
        require_id(self.position_type, "position_type")
        require_nonempty(self.encoded_value, "encoded_value")
        require_digest(self.coverage_digest, "coverage_digest")


@dataclass(frozen=True, slots=True)
class VerificationProof:
    proof_id: str
    proof_type: str
    producer: str
    subject_digest: str
    status: VerificationStatus
    produced_at: str
    details_digest: str
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        require_id(self.proof_id, "proof_id")
        require_id(self.proof_type, "proof_type")
        require_id(self.producer, "producer")
        require_digest(self.subject_digest, "subject_digest")
        require_digest(self.details_digest, "details_digest")
        require_nonempty(self.produced_at, "produced_at")


@dataclass(frozen=True, slots=True)
class EffectReceipt:
    effect_id: str
    provider: str
    capability: str
    operation_digest: str
    readback_digest: str
    verification_proof_id: str
    verified_at: str

    def __post_init__(self) -> None:
        require_id(self.effect_id, "effect_id")
        require_id(self.provider, "provider")
        require_id(self.capability, "capability")
        require_digest(self.operation_digest, "operation_digest")
        require_digest(self.readback_digest, "readback_digest")
        require_id(self.verification_proof_id, "verification_proof_id")
        require_nonempty(self.verified_at, "verified_at")


@dataclass(frozen=True, slots=True)
class CommitCohort:
    cohort_id: str
    source_ids: tuple[str, ...]
    output_ids: tuple[str, ...]
    effect_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_id(self.cohort_id, "cohort_id")
        # V2-RATCHET-010 requires every proposed position, effect, proof, and
        # candidate to belong to a cohort -- it does not require every cohort to
        # own a source. A publish-only cohort that advances no source position is
        # legitimate, so the rule is "at least one member of any kind". Demanding
        # a source here also made Ratchet's own emptiness check unreachable.
        if not (self.source_ids or self.output_ids or self.effect_ids):
            raise ValueError("a commit cohort requires at least one source, output, or effect")
        require_unique(self.source_ids, "source_ids")
        require_unique(self.output_ids, "output_ids")
        require_unique(self.effect_ids, "effect_ids")


@dataclass(frozen=True, slots=True)
class RunCommitBundle:
    instance_id: str
    run_id: str
    attempt_id: str
    plan_digest: str
    effective_config_digest: str
    runtime_lock_digest: str
    candidate_snapshot_digest: str
    coverage_digests: tuple[str, ...]
    disposition_digest: str
    cohort_manifest_digest: str
    proposed_positions: tuple[ProposedPosition, ...]
    proofs: tuple[VerificationProof, ...]
    effect_receipts: tuple[EffectReceipt, ...] = ()
    exception_ids: tuple[str, ...] = ()
    # V2-EXC-005: the *rule* each exception waives, not just its opaque id.
    # Ratchet owns the structural invariants and must be able to see what a
    # waiver claims to excuse; matching against ids alone is unenforceable
    # because ids are opaque by construction.
    waived_rule_ids: tuple[str, ...] = ()
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        require_id(self.instance_id, "instance_id")
        require_run_id(self.run_id)
        require_id(self.attempt_id, "attempt_id")
        for name in (
            "plan_digest",
            "effective_config_digest",
            "runtime_lock_digest",
            "candidate_snapshot_digest",
            "disposition_digest",
            "cohort_manifest_digest",
        ):
            require_digest(getattr(self, name), name)
        for digest in self.coverage_digests:
            require_digest(digest, "coverage_digest")
        require_unique(self.coverage_digests, "coverage_digests")
        require_unique((position.source_id for position in self.proposed_positions), "position sources")
        require_unique((proof.proof_id for proof in self.proofs), "proof ids")
        require_unique((receipt.effect_id for receipt in self.effect_receipts), "effect ids")
        require_unique(self.exception_ids, "exception_ids")
        structural = sorted(set(self.waived_rule_ids) & NON_WAIVABLE_REASON_CODES)
        if structural:
            raise ValueError(
                f"commit bundle attempts to waive non-waivable safety: {structural}"
            )

    @property
    def digest(self) -> str:
        return canonical_digest(self)


@dataclass(frozen=True, slots=True)
class CommitCertificate:
    certificate_id: str
    instance_id: str
    run_id: str
    attempt_id: str
    bundle_digest: str
    committed_at: str
    outcome: TerminalOutcome
    active_snapshot_digest: str
    committed_position_digest: str
    previous_certificate_digest: str | None = None

    def __post_init__(self) -> None:
        require_id(self.certificate_id, "certificate_id")
        require_id(self.instance_id, "instance_id")
        require_run_id(self.run_id)
        require_id(self.attempt_id, "attempt_id")
        require_digest(self.bundle_digest, "bundle_digest")
        require_nonempty(self.committed_at, "committed_at")
        require_digest(self.active_snapshot_digest, "active_snapshot_digest")
        require_digest(self.committed_position_digest, "committed_position_digest")
        if self.previous_certificate_digest is not None:
            require_digest(self.previous_certificate_digest, "previous_certificate_digest")
