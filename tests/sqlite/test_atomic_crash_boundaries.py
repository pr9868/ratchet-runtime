"""Kill real processes after checkpoint, verified receipt, staging and commit."""
import multiprocessing as mp
import os
import signal
import sys
import pytest
from ratchet_sqlite import SQLiteRatchet
from ratchet_sqlite.contracts import CheckpointTier, CommitCohort, EffectReceipt, TerminalOutcome
from test_ratchet import attempt, digest, make_bundle, proof


def worker(database, boundary):
    runtime = SQLiteRatchet(database)
    runtime.initialize_instance("demo")
    lease = runtime.acquire("demo", "worker")
    handle = attempt(runtime, lease)
    runtime.record_checkpoint(lease, handle, boundary_tier=CheckpointTier.PHASE,
        boundary_id="read", proof=proof(digest("read"), "read"))
    if boundary == "checkpoint":
        os.kill(os.getpid(), signal.SIGKILL)
    operation = digest("operation")
    runtime.prepare_effect(lease, handle, effect_id="write", provider="fake", capability="write",
        operation_digest=operation, idempotency_key="write-once")
    runtime.mark_effect_dispatching(lease, handle, effect_id="write")
    readback = proof(digest("readback"), "readback")
    receipt = EffectReceipt("write", "fake", "write", operation, readback.subject_digest,
                            readback.proof_id, "2026-09-29T00:00:00Z")
    runtime.record_effect_receipt(lease, handle, receipt=receipt, proof=readback)
    if boundary == "receipt":
        os.kill(os.getpid(), signal.SIGKILL)
    candidate = digest("candidate")
    cohort = CommitCohort("cohort", (), (candidate,), ("write",))
    bundle = make_bundle(handle, positions=(), cohort=cohort, candidate=candidate,
        disposition=digest("disposition"), coverage=(), extra_proofs=(readback,), receipts=(receipt,))
    runtime.stage_bundle(lease, handle, bundle, cohorts=(cohort,))
    if boundary == "staged":
        os.kill(os.getpid(), signal.SIGKILL)
    runtime.commit_bundle(lease, handle, bundle, cohorts=(cohort,), outcome=TerminalOutcome.COMPLETED)
    os.kill(os.getpid(), signal.SIGKILL)


@pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX SIGKILL")
@pytest.mark.parametrize("boundary", ["checkpoint", "receipt", "staged", "committed"])
def test_durable_boundary_survives_sigkill(tmp_path, boundary):
    database = tmp_path / "state.sqlite3"
    process = mp.get_context("spawn").Process(target=worker, args=(database, boundary))
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("crash fixture timed out")
    assert process.exitcode == -signal.SIGKILL
    reopened = SQLiteRatchet(database)
    status = reopened.status("demo")
    assert len(reopened.list_checkpoints("demo", status.attempts[0].attempt_id)) == 1
    if boundary != "checkpoint":
        assert reopened.effect_recovery("demo", "write").action == "no_action"
    if boundary == "committed":
        assert status.active_snapshot.snapshot_digest == digest("candidate")
        assert len(reopened.certificate_chain("demo")) == 1
    else:
        assert status.active_snapshot is None
        assert reopened.certificate_chain("demo") == ()
