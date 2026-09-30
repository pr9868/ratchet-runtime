"""Synthetic atomic commit and inspection example; writes only a temporary folder."""
from pathlib import Path
from tempfile import TemporaryDirectory
from ratchet_sqlite import SQLiteRatchet
from ratchet_sqlite.contracts import (
    CheckpointTier, CommitCohort, ProposedPosition, RunCommitBundle,
    TerminalOutcome, VerificationProof, VerificationStatus, canonical_digest,
)


def main():
    with TemporaryDirectory(prefix="ratchet-example-") as raw:
        database = Path(raw) / "state.sqlite3"
        runtime = SQLiteRatchet(database)
        runtime.initialize_instance("demo")
        lease = runtime.acquire("demo", "example")
        handle = runtime.begin_attempt(
            lease, run_id="run-001", plan_digest=canonical_digest("example-plan"),
            effective_config_digest=canonical_digest({"mode": "full"}),
            runtime_lock_digest=canonical_digest("example-host-v1"),
            requested_tier=CheckpointTier.PHASE, minimum_tier=CheckpointTier.RUN,
        )
        # An application would durably write its artifact and verify its contents.
        artifact = Path(raw) / "snapshot.txt"
        artifact.write_text("Synthetic verified result\n")
        expected = "Synthetic verified result\n"
        assert artifact.read_text() == expected
        candidate = canonical_digest(expected)
        coverage = canonical_digest({"records_read": [1]})
        disposition = canonical_digest({"records_applied": [1]})
        position = ProposedPosition("input", "integer", "1", coverage)
        runtime.propose_positions(lease, handle, (position,))
        cohort = CommitCohort("cohort", ("input",), (candidate,))
        cohort_digest = canonical_digest((cohort,))
        subjects = (candidate, coverage, disposition, cohort_digest)
        # These structural proofs are synthetic and deliberately do not claim
        # verification of any real provider or agent-generated business fact.
        proofs = tuple(VerificationProof(
            proof_id=f"proof-{i}", proof_type="structural", producer="example",
            subject_digest=subject, status=VerificationStatus.VERIFIED,
            produced_at="2026-09-29T00:00:00Z", details_digest=canonical_digest({"check": i}),
        ) for i, subject in enumerate(subjects))
        bundle = RunCommitBundle(
            instance_id=handle.instance_id, run_id=handle.run_id, attempt_id=handle.attempt_id,
            plan_digest=handle.plan_digest, effective_config_digest=handle.effective_config_digest,
            runtime_lock_digest=handle.runtime_lock_digest, candidate_snapshot_digest=candidate,
            coverage_digests=(coverage,), disposition_digest=disposition,
            cohort_manifest_digest=cohort_digest, proposed_positions=(position,), proofs=proofs,
        )
        certificate = runtime.commit_bundle(lease, handle, bundle, cohorts=(cohort,),
            outcome=TerminalOutcome.COMPLETED, required_proof_types=("structural",))
        assert runtime.commit_bundle(lease, handle, bundle, cohorts=(cohort,),
            outcome=TerminalOutcome.COMPLETED, required_proof_types=("structural",)) == certificate
        runtime.release(lease)
        readonly = SQLiteRatchet.open_read_only(database)
        try:
            status = readonly.status("demo")
            assert status.active_snapshot.snapshot_digest == candidate
            assert status.committed_positions[0].encoded_value == "1"
            assert readonly.certificate_chain("demo") == (certificate,)
        finally:
            readonly.close()
        print("SQLite commit, exact replay, position and read-only inspection passed.")


if __name__ == "__main__":
    main()
