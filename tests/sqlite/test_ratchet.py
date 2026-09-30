from __future__ import annotations

from dataclasses import replace
import sqlite3

import pytest
import ratchet_sqlite.backend as backend_module
from ratchet_sqlite.contracts import (
    CheckpointTier,
    CommitCohort,
    EffectReceipt,
    ProposedPosition,
    RunIdValidationError,
    RunCommitBundle,
    TerminalOutcome,
    VerificationProof,
    VerificationStatus,
    canonical_digest,
)

from ratchet_sqlite import (
    AlreadyCommitted,
    AttemptConflict,
    BundleRejected,
    CohortIncomplete,
    FenceRejected,
    PositionRegression,
    ReadbackRequired,
    ReadOnlyRatchetError,
    SQLiteRatchet,
    effective_checkpoint_tier,
)


class Clock:
    def __init__(self, value: float = 1_800_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def digest(label: str) -> str:
    return canonical_digest({"fixture": label})


def proof(subject: str, label: str, *, proof_type: str = "structural") -> VerificationProof:
    return VerificationProof(
        proof_id=f"proof-{label}",
        proof_type=proof_type,
        producer="ratchet-test",
        subject_digest=subject,
        status=VerificationStatus.VERIFIED,
        produced_at="2027-01-15T08:00:00.000000Z",
        details_digest=digest(f"proof-details-{label}"),
    )


def attempt(ratchet: SQLiteRatchet, lease, run_id: str = "run-1"):
    return ratchet.begin_attempt(
        lease,
        run_id=run_id,
        plan_digest=digest(f"{run_id}-plan"),
        effective_config_digest=digest(f"{run_id}-config"),
        runtime_lock_digest=digest(f"{run_id}-runtime"),
        requested_tier=CheckpointTier.PHASE,
        minimum_tier=CheckpointTier.RUN,
    )


def make_bundle(
    active_attempt,
    *,
    positions: tuple[ProposedPosition, ...],
    cohort: CommitCohort,
    candidate: str,
    disposition: str,
    coverage: tuple[str, ...],
    extra_proofs: tuple[VerificationProof, ...] = (),
    receipts: tuple[EffectReceipt, ...] = (),
) -> RunCommitBundle:
    cohort_digest = canonical_digest((cohort,))
    structural_subjects = tuple(dict.fromkeys((candidate, disposition, cohort_digest, *coverage)))
    structural_proofs = tuple(
        proof(subject, f"structural-{index}")
        for index, subject in enumerate(structural_subjects)
    )
    return RunCommitBundle(
        instance_id=active_attempt.instance_id,
        run_id=active_attempt.run_id,
        attempt_id=active_attempt.attempt_id,
        plan_digest=active_attempt.plan_digest,
        effective_config_digest=active_attempt.effective_config_digest,
        runtime_lock_digest=active_attempt.runtime_lock_digest,
        candidate_snapshot_digest=candidate,
        coverage_digests=coverage,
        disposition_digest=disposition,
        cohort_manifest_digest=cohort_digest,
        proposed_positions=positions,
        proofs=structural_proofs + extra_proofs,
        effect_receipts=receipts,
    )


def test_instance_isolation_and_fencing_are_atomic(tmp_path) -> None:
    clock = Clock()
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3", clock=clock)
    ratchet.initialize_instance("one")
    ratchet.initialize_instance("two")

    first = ratchet.acquire("one", "worker-a", ttl_seconds=10)
    other = ratchet.acquire("two", "worker-b", ttl_seconds=10)
    assert first.fencing_token == 1
    assert other.fencing_token == 1

    clock.advance(11)
    replacement = ratchet.acquire("one", "worker-c", ttl_seconds=10)
    assert replacement.fencing_token == 2
    with pytest.raises(FenceRejected):
        attempt(ratchet, first)

    live = attempt(ratchet, replacement)
    assert live.instance_id == "one"
    assert ratchet.status("two").attempts == ()


def test_ratchet_rejects_noncanonical_run_id_before_attempt_write(tmp_path) -> None:
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")

    with pytest.raises(RunIdValidationError) as failure:
        attempt(ratchet, lease, "refresh-20260901T013031Z")

    assert failure.value.code == "run_id.invalid"
    assert ratchet.status("instance").attempts == ()


def test_read_only_ratchet_opens_enforced_read_only_state_without_side_effects(
    tmp_path,
) -> None:
    database = tmp_path / "ratchet.sqlite3"
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("instance")
    before_mode = database.stat().st_mode & 0o777
    parent_mode = tmp_path.stat().st_mode & 0o777
    database.chmod(0o444)
    tmp_path.chmod(0o555)

    def fingerprint():
        return tuple(
            (
                path.name,
                path.read_bytes(),
                path.stat().st_size,
                path.stat().st_mtime_ns,
                path.stat().st_ctime_ns,
            )
            for path in sorted(tmp_path.iterdir())
            if path.is_file()
        )

    try:
        before = fingerprint()
        read_only = SQLiteRatchet.open_read_only(database)
        assert read_only.read_only is True
        assert read_only.status("instance").instance_id == "instance"
        with pytest.raises(ReadOnlyRatchetError):
            read_only.initialize_instance("other")
        assert fingerprint() == before
        # SQLite versions differ on whether a completed writer leaves empty WAL/SHM
        # sidecars behind.  The exact fingerprint comparison above proves that the
        # read-only open neither creates nor changes whichever initial state exists.
    finally:
        tmp_path.chmod(parent_mode)
        database.chmod(before_mode)


def test_read_only_ratchet_reads_existing_wal_without_creating_sidecars(tmp_path) -> None:
    database = tmp_path / "ratchet.sqlite3"
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("base")
    keeper = sqlite3.connect(database)
    keeper.execute("PRAGMA journal_mode=WAL")
    keeper.execute(
        "INSERT INTO instances(instance_id, next_fencing_token, created_at) VALUES (?, 0, ?)",
        ("wal-instance", "2027-01-15T08:00:00.000000Z"),
    )
    keeper.commit()
    sidecars = (
        database,
        database.with_name(f"{database.name}-wal"),
        database.with_name(f"{database.name}-shm"),
    )
    assert all(path.is_file() for path in sidecars)
    unsafe = tmp_path / "missing-shm"
    unsafe.mkdir()
    unsafe_database = unsafe / database.name
    unsafe_database.write_bytes(database.read_bytes())
    unsafe_database.with_name(f"{database.name}-wal").write_bytes(sidecars[1].read_bytes())
    with pytest.raises(RuntimeError, match="without its existing SHM sidecar"):
        SQLiteRatchet.open_read_only(unsafe_database)
    modes = {path: path.stat().st_mode & 0o777 for path in sidecars}
    parent_mode = tmp_path.stat().st_mode & 0o777
    for path in sidecars:
        path.chmod(0o444)
    tmp_path.chmod(0o555)
    try:
        before = tuple(
            (
                path.name,
                path.read_bytes(),
                path.stat().st_size,
                path.stat().st_mtime_ns,
                path.stat().st_ctime_ns,
            )
            for path in sidecars
        )
        read_only = SQLiteRatchet.open_read_only(database)
        assert read_only.status("wal-instance").instance_id == "wal-instance"
        after = tuple(
            (
                path.name,
                path.read_bytes(),
                path.stat().st_size,
                path.stat().st_mtime_ns,
                path.stat().st_ctime_ns,
            )
            for path in sidecars
        )
        assert after == before
    finally:
        tmp_path.chmod(parent_mode)
        for path in sidecars:
            path.chmod(modes[path])
        keeper.close()


def test_schema_initialization_rolls_back_if_ddl_is_interrupted(
    tmp_path, monkeypatch
) -> None:
    database = tmp_path / "ratchet.sqlite3"
    execute_schema = backend_module._execute_atomic_script

    def fail_after_first_statement(connection, script):
        connection.execute("CREATE TABLE leaked_partial_schema(value TEXT)")
        raise RuntimeError("injected schema failure")

    monkeypatch.setattr(backend_module, "_execute_atomic_script", fail_after_first_statement)
    with pytest.raises(RuntimeError, match="injected schema failure"):
        SQLiteRatchet(database)

    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert "leaked_partial_schema" not in tables
    assert "ratchet_meta" not in tables

    monkeypatch.setattr(backend_module, "_execute_atomic_script", execute_schema)
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("instance")
    assert ratchet.status("instance").instance_id == "instance"


def test_attempt_recovery_preserves_all_three_binding_digests(tmp_path) -> None:
    clock = Clock()
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3", clock=clock)
    ratchet.initialize_instance("instance")
    old_lease = ratchet.acquire("instance", "old", ttl_seconds=5)
    original = attempt(ratchet, old_lease)
    clock.advance(6)
    new_lease = ratchet.acquire("instance", "new", ttl_seconds=30)

    with pytest.raises(FenceRejected):
        ratchet.propose_positions(
            old_lease,
            original,
            (ProposedPosition("source", "integer", "1", digest("coverage")),),
        )

    with pytest.raises(AttemptConflict):
        ratchet.recover_attempt(
            new_lease,
            attempt_id=original.attempt_id,
            plan_digest=digest("different-plan"),
            effective_config_digest=original.effective_config_digest,
            runtime_lock_digest=original.runtime_lock_digest,
        )

    recovered = ratchet.recover_attempt(
        new_lease,
        attempt_id=original.attempt_id,
        plan_digest=original.plan_digest,
        effective_config_digest=original.effective_config_digest,
        runtime_lock_digest=original.runtime_lock_digest,
    )
    assert recovered.fencing_token == new_lease.fencing_token
    assert recovered.plan_digest == original.plan_digest


def test_checkpoint_tier_escalates_and_skips_finer_boundaries(tmp_path) -> None:
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = ratchet.begin_attempt(
        lease,
        run_id="run-tier",
        plan_digest=digest("plan"),
        effective_config_digest=digest("config"),
        runtime_lock_digest=digest("runtime"),
        requested_tier=CheckpointTier.RUN,
        minimum_tier=CheckpointTier.NODE,
    )
    assert effective_checkpoint_tier(
        CheckpointTier.RUN, CheckpointTier.NODE
    ) is CheckpointTier.NODE
    assert active_attempt.effective_tier is CheckpointTier.NODE
    checkpoint_proof = proof(digest("node-output"), "node")
    assert ratchet.record_checkpoint(
        lease,
        active_attempt,
        boundary_tier=CheckpointTier.NODE,
        boundary_id="node-1",
        proof=checkpoint_proof,
    )
    assert not ratchet.record_checkpoint(
        lease,
        active_attempt,
        boundary_tier=CheckpointTier.FORENSIC,
        boundary_id="batch-1",
        proof=checkpoint_proof,
    )
    checkpoints = ratchet.list_checkpoints("instance", active_attempt.attempt_id)
    assert len(checkpoints) == 1
    assert checkpoints[0].boundary_tier is CheckpointTier.NODE


@pytest.mark.parametrize("tier", tuple(CheckpointTier))
def test_checkpoint_tier_never_weakens_commit_verification(tmp_path, tier) -> None:
    ratchet = SQLiteRatchet(tmp_path / f"ratchet-{tier.value}.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = ratchet.begin_attempt(
        lease,
        run_id=f"run-{tier.value}",
        plan_digest=digest("plan"),
        effective_config_digest=digest("config"),
        runtime_lock_digest=digest("runtime"),
        requested_tier=tier,
        minimum_tier=CheckpointTier.RUN,
    )
    coverage = digest(f"coverage-{tier.value}")
    position = ProposedPosition("source", "integer", "1", coverage)
    ratchet.propose_positions(lease, active_attempt, (position,))
    candidate = digest(f"candidate-{tier.value}")
    disposition = digest(f"disposition-{tier.value}")
    cohort = CommitCohort("cohort", ("source",), (candidate,))
    complete = make_bundle(
        active_attempt,
        positions=(position,),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(coverage,),
    )
    incomplete = replace(
        complete,
        proofs=tuple(
            proof for proof in complete.proofs if proof.subject_digest != coverage
        ),
    )
    with pytest.raises(BundleRejected, match="lacks verified proof subjects"):
        ratchet.stage_bundle(
            lease,
            active_attempt,
            incomplete,
            cohorts=(cohort,),
        )


def test_effect_recovery_requires_readback_after_uncertain_dispatch(tmp_path) -> None:
    clock = Clock()
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3", clock=clock)
    ratchet.initialize_instance("instance")
    old_lease = ratchet.acquire("instance", "old", ttl_seconds=5)
    original = attempt(ratchet, old_lease, "run-effect")
    operation_digest = digest("operation")
    ratchet.prepare_effect(
        old_lease,
        original,
        effect_id="effect-1",
        provider="opaque-provider-id",
        capability="opaque-capability-id",
        operation_digest=operation_digest,
        idempotency_key="instance/run-effect/effect-1",
    )
    ratchet.mark_effect_dispatching(old_lease, original, effect_id="effect-1")
    with pytest.raises(ReadbackRequired):
        ratchet.mark_effect_dispatching(old_lease, original, effect_id="effect-1")

    clock.advance(6)
    new_lease = ratchet.acquire("instance", "new", ttl_seconds=30)
    recovered = ratchet.recover_attempt(
        new_lease,
        attempt_id=original.attempt_id,
        plan_digest=original.plan_digest,
        effective_config_digest=original.effective_config_digest,
        runtime_lock_digest=original.runtime_lock_digest,
    )
    recovery = ratchet.effect_recovery("instance", "effect-1")
    assert recovery.action == "readback_required"

    readback = digest("readback")
    readback_proof = proof(readback, "effect-readback", proof_type="effect-readback")
    receipt = EffectReceipt(
        effect_id="effect-1",
        provider="opaque-provider-id",
        capability="opaque-capability-id",
        operation_digest=operation_digest,
        readback_digest=readback,
        verification_proof_id=readback_proof.proof_id,
        verified_at="2027-01-15T08:00:01.000000Z",
    )
    ratchet.record_effect_receipt(
        new_lease, recovered, receipt=receipt, proof=readback_proof
    )
    assert ratchet.effect_recovery("instance", "effect-1").action == "no_action"


def test_cohort_commit_is_all_or_none_and_position_commit_is_atomic(tmp_path) -> None:
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = attempt(ratchet, lease, "run-cohort")
    coverage_a = digest("coverage-a")
    coverage_b = digest("coverage-b")
    position_a = ProposedPosition("source-a", "integer", "5", coverage_a)
    position_b = ProposedPosition("source-b", "integer", "7", coverage_b)
    ratchet.propose_positions(lease, active_attempt, (position_a,))
    assert ratchet.list_proposed_positions("instance", active_attempt.attempt_id) == (
        position_a,
    )
    candidate = digest("candidate")
    disposition = digest("disposition")
    cohort = CommitCohort(
        cohort_id="cohort-1",
        source_ids=("source-a", "source-b"),
        output_ids=(candidate,),
    )
    partial = make_bundle(
        active_attempt,
        positions=(position_a,),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(coverage_a,),
    )
    with pytest.raises(CohortIncomplete):
        ratchet.commit_bundle(
            lease,
            active_attempt,
            partial,
            cohorts=(cohort,),
            outcome=next(iter(TerminalOutcome)),
        )
    assert ratchet.status("instance").committed_positions == ()

    ratchet.propose_positions(lease, active_attempt, (position_b,))
    complete = make_bundle(
        active_attempt,
        positions=(position_a, position_b),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(coverage_a, coverage_b),
    )
    certificate = ratchet.commit_bundle(
        lease,
        active_attempt,
        complete,
        cohorts=(cohort,),
        outcome=next(iter(TerminalOutcome)),
    )
    assert (
        ratchet.commit_bundle(
            lease,
            active_attempt,
            complete,
            cohorts=(cohort,),
            outcome=next(iter(TerminalOutcome)),
        )
        == certificate
    )
    status = ratchet.status("instance")
    assert status.active_snapshot is not None
    assert status.active_snapshot.snapshot_digest == candidate
    assert status.active_snapshot.certificate_id == certificate.certificate_id
    assert {position.source_id for position in status.committed_positions} == {
        "source-a",
        "source-b",
    }
    assert ratchet.get_certificate("instance", certificate.certificate_id) == certificate

    reopened = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    reopened_status = reopened.status("instance")
    assert reopened_status.active_snapshot == status.active_snapshot
    assert reopened.get_certificate("instance", certificate.certificate_id) == certificate

    next_attempt = attempt(ratchet, lease, "run-regression")
    with pytest.raises(PositionRegression):
        ratchet.propose_positions(
            lease,
            next_attempt,
            (ProposedPosition("source-a", "integer", "4", digest("later-coverage")),),
        )


def test_certificate_chain_is_linear_inspectable_and_digest_verified(tmp_path) -> None:
    database = tmp_path / "ratchet.sqlite3"
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")

    certificates = []
    for number in (1, 2):
        active_attempt = attempt(ratchet, lease, f"run-{number}")
        coverage = digest(f"coverage-{number}")
        position = ProposedPosition("source", "integer", str(number), coverage)
        ratchet.propose_positions(lease, active_attempt, (position,))
        candidate = digest(f"candidate-{number}")
        disposition = digest(f"disposition-{number}")
        cohort = CommitCohort(f"cohort-{number}", ("source",), (candidate,))
        bundle = make_bundle(
            active_attempt,
            positions=(position,),
            cohort=cohort,
            candidate=candidate,
            disposition=disposition,
            coverage=(coverage,),
        )
        certificates.append(
            ratchet.commit_bundle(
                lease,
                active_attempt,
                bundle,
                cohorts=(cohort,),
                outcome=next(iter(TerminalOutcome)),
            )
        )

    assert ratchet.certificate_chain("instance") == tuple(certificates)
    assert certificates[1].previous_certificate_digest == canonical_digest(certificates[0])

    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            UPDATE certificates SET certificate_digest = ?
            WHERE instance_id = ? AND certificate_id = ?
            """,
            ("0" * 64, "instance", certificates[0].certificate_id),
        )
    with pytest.raises(BundleRejected, match="digest verification failed"):
        ratchet.certificate_chain("instance")


def test_bundle_requires_typed_verified_structural_proofs_and_is_immutable(tmp_path) -> None:
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = attempt(ratchet, lease, "run-bundle")
    coverage = digest("coverage")
    proposed = ProposedPosition("source", "integer", "1", coverage)
    ratchet.propose_positions(lease, active_attempt, (proposed,))
    candidate = digest("candidate")
    disposition = digest("disposition")
    cohort = CommitCohort("cohort", ("source",), (candidate,))
    complete = make_bundle(
        active_attempt,
        positions=(proposed,),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(coverage,),
    )
    missing_coverage_proof = replace(
        complete,
        proofs=tuple(
            value for value in complete.proofs if value.subject_digest != coverage
        ),
    )
    with pytest.raises(BundleRejected):
        ratchet.stage_bundle(
            lease, active_attempt, missing_coverage_proof, cohorts=(cohort,)
        )

    staged_digest = ratchet.stage_bundle(
        lease,
        active_attempt,
        complete,
        cohorts=(cohort,),
        required_proof_types=("structural",),
    )
    assert staged_digest == complete.digest
    assert (
        ratchet.stage_bundle(
            lease,
            active_attempt,
            complete,
            cohorts=(cohort,),
            required_proof_types=("structural",),
        )
        == staged_digest
    )

    other_candidate = digest("other-candidate")
    changed_cohort = CommitCohort("cohort", ("source",), (other_candidate,))
    changed = make_bundle(
        active_attempt,
        positions=(proposed,),
        cohort=changed_cohort,
        candidate=other_candidate,
        disposition=disposition,
        coverage=(coverage,),
    )
    with pytest.raises(AlreadyCommitted):
        ratchet.stage_bundle(
            lease,
            active_attempt,
            changed,
            cohorts=(changed_cohort,),
            required_proof_types=("structural",),
        )


def test_verified_effect_receipt_is_required_by_its_commit_cohort(tmp_path) -> None:
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = attempt(ratchet, lease, "run-publish")
    operation = digest("publish-operation")
    ratchet.prepare_effect(
        lease,
        active_attempt,
        effect_id="publish-1",
        provider="surface",
        capability="write",
        operation_digest=operation,
        idempotency_key="publish-1-once",
    )
    ratchet.mark_effect_dispatching(lease, active_attempt, effect_id="publish-1")
    candidate = digest("candidate")
    disposition = digest("disposition")
    # Publish-only cohort: no source position advances, but the effect must still
    # be verified before the certificate is issued.
    cohort = CommitCohort("publish-cohort", (), (candidate,), ("publish-1",))
    before_readback = make_bundle(
        active_attempt,
        positions=(),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(),
    )
    with pytest.raises((CohortIncomplete, BundleRejected)):
        ratchet.commit_bundle(
            lease,
            active_attempt,
            before_readback,
            cohorts=(cohort,),
            outcome=next(iter(TerminalOutcome)),
        )

    readback = digest("publish-readback")
    effect_proof = proof(readback, "publish-readback", proof_type="effect-readback")
    receipt = EffectReceipt(
        effect_id="publish-1",
        provider="surface",
        capability="write",
        operation_digest=operation,
        readback_digest=readback,
        verification_proof_id=effect_proof.proof_id,
        verified_at="2027-01-15T08:00:01.000000Z",
    )
    ratchet.record_effect_receipt(
        lease, active_attempt, receipt=receipt, proof=effect_proof
    )
    verified = make_bundle(
        active_attempt,
        positions=(),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(),
        extra_proofs=(effect_proof,),
        receipts=(receipt,),
    )
    certificate = ratchet.commit_bundle(
        lease,
        active_attempt,
        verified,
        cohorts=(cohort,),
        outcome=next(iter(TerminalOutcome)),
        required_proof_types=("structural", "effect-readback"),
    )
    assert certificate.active_snapshot_digest == candidate
    assert ratchet.recovery_status("instance").effects[0].action == "no_action"


def test_stale_attempt_cannot_commit_a_position_behind_the_committed_one(tmp_path) -> None:
    """V2-RATCHET-004 regression.

    Monotonicity was previously checked only when a position was proposed. A
    lapsed attempt could therefore propose position 5, have a newer run commit
    position 9, then be recovered under a newer fencing token and commit 5 --
    walking the source cursor backwards and silently re-reading old evidence.
    """
    clock = Clock()
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3", clock=clock)
    ratchet.initialize_instance("instance")

    coverage_old = digest("coverage-old")
    coverage_new = digest("coverage-new")
    candidate_old = digest("candidate-old")
    candidate_new = digest("candidate-new")
    disposition = digest("disposition")

    # A stale attempt proposes an early position while it is still valid.
    stale_lease = ratchet.acquire("instance", "worker-one", ttl_seconds=10)
    stale_attempt = attempt(ratchet, stale_lease, "run-stale")
    behind = ProposedPosition("source-a", "integer", "5", coverage_old)
    ratchet.propose_positions(stale_lease, stale_attempt, (behind,))

    # That tenure lapses and a newer run commits a position ahead of it.
    clock.advance(11)
    ahead_lease = ratchet.acquire("instance", "worker-two", ttl_seconds=10)
    ahead_attempt = attempt(ratchet, ahead_lease, "run-ahead")
    ahead = ProposedPosition("source-a", "integer", "9", coverage_new)
    ratchet.propose_positions(ahead_lease, ahead_attempt, (ahead,))
    ahead_cohort = CommitCohort(
        cohort_id="cohort-ahead",
        source_ids=("source-a",),
        output_ids=(candidate_new,),
    )
    ratchet.commit_bundle(
        ahead_lease,
        ahead_attempt,
        make_bundle(
            ahead_attempt,
            positions=(ahead,),
            cohort=ahead_cohort,
            candidate=candidate_new,
            disposition=disposition,
            coverage=(coverage_new,),
        ),
        cohorts=(ahead_cohort,),
        outcome=next(iter(TerminalOutcome)),
    )
    assert ratchet.status("instance").committed_positions[0].encoded_value == "9"

    # The stale attempt is adopted under the newer fencing token. Its proposal
    # of position 5 is already durable, so bundle validation accepts it; only a
    # commit-time monotonicity check can stop it.
    replay_attempt = ratchet.recover_attempt(
        ahead_lease,
        attempt_id=stale_attempt.attempt_id,
        plan_digest=stale_attempt.plan_digest,
        effective_config_digest=stale_attempt.effective_config_digest,
        runtime_lock_digest=stale_attempt.runtime_lock_digest,
    )
    stale_cohort = CommitCohort(
        cohort_id="cohort-stale",
        source_ids=("source-a",),
        output_ids=(candidate_old,),
    )
    with pytest.raises(PositionRegression):
        ratchet.commit_bundle(
            ahead_lease,
            replay_attempt,
            make_bundle(
                replay_attempt,
                positions=(behind,),
                cohort=stale_cohort,
                candidate=candidate_old,
                disposition=disposition,
                coverage=(coverage_old,),
            ),
            cohorts=(stale_cohort,),
            outcome=next(iter(TerminalOutcome)),
        )

    # The committed position is unchanged.
    assert ratchet.status("instance").committed_positions[0].encoded_value == "9"


def test_bundle_declaring_a_structural_waiver_is_refused(tmp_path) -> None:
    """V2-EXC-005 enforced where the invariants live.

    An earlier version matched non-waivable codes against opaque exception ids,
    which never fired. The bundle now declares the rules its exceptions waive,
    so Ratchet can actually see -- and refuse -- a structural waiver.
    """
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = attempt(ratchet, lease, "run-waiver")

    candidate = digest("candidate")
    disposition = digest("disposition")
    cohort = CommitCohort("cohort-1", ("source-a",), (candidate,))
    coverage = digest("coverage-a")
    position = ProposedPosition("source-a", "integer", "5", coverage)
    ratchet.propose_positions(lease, active_attempt, (position,))

    bundle = make_bundle(
        active_attempt,
        positions=(position,),
        cohort=cohort,
        candidate=candidate,
        disposition=disposition,
        coverage=(coverage,),
    )
    # The contract refuses to construct such a bundle at all...
    with pytest.raises(ValueError, match="non-waivable safety"):
        replace(bundle, exception_ids=("exception-1",), waived_rule_ids=("fencing_failure",))

    # ...and Ratchet refuses it independently, so a caller that bypasses contract
    # validation still cannot commit a structural waiver. Defence in depth is only
    # real if the second layer is verified separately from the first.
    hostile = replace(bundle, exception_ids=("exception-1",), waived_rule_ids=("cursor_atomicity_x",))
    object.__setattr__(hostile, "waived_rule_ids", ("fencing_failure",))
    with pytest.raises(BundleRejected, match="non-waivable safety"):
        ratchet.commit_bundle(
            lease,
            active_attempt,
            hostile,
            cohorts=(cohort,),
            outcome=next(iter(TerminalOutcome)),
        )


def test_exceptions_must_declare_the_rules_they_waive(tmp_path) -> None:
    """An undeclared waiver is unauditable, so it is refused."""
    ratchet = SQLiteRatchet(tmp_path / "ratchet.sqlite3")
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker")
    active_attempt = attempt(ratchet, lease, "run-undeclared")

    candidate = digest("candidate")
    coverage = digest("coverage-a")
    cohort = CommitCohort("cohort-1", ("source-a",), (candidate,))
    position = ProposedPosition("source-a", "integer", "5", coverage)
    ratchet.propose_positions(lease, active_attempt, (position,))

    bundle = replace(
        make_bundle(
            active_attempt,
            positions=(position,),
            cohort=cohort,
            candidate=candidate,
            disposition=digest("disposition"),
            coverage=(coverage,),
        ),
        exception_ids=("exception-1",),
    )
    with pytest.raises(BundleRejected, match="without declaring which rules"):
        ratchet.commit_bundle(
            lease,
            active_attempt,
            bundle,
            cohorts=(cohort,),
            outcome=next(iter(TerminalOutcome)),
        )
