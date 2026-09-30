from dataclasses import replace
import pytest
from ratchet_sqlite import SQLiteRatchet, BundleRejected, AlreadyCommitted
from ratchet_sqlite.contracts import CommitCohort, TerminalOutcome, canonical_digest
from test_ratchet import attempt, digest, make_bundle


def fixture(tmp_path):
    runtime = SQLiteRatchet(tmp_path / "state.sqlite3")
    runtime.initialize_instance("demo")
    lease = runtime.acquire("demo", "worker")
    handle = attempt(runtime, lease)
    candidate = digest("candidate")
    cohort = CommitCohort("cohort", (), (candidate,))
    bundle = make_bundle(handle, positions=(), cohort=cohort, candidate=candidate,
                         disposition=digest("disposition"), coverage=())
    return runtime, lease, handle, cohort, bundle


@pytest.mark.parametrize("outcome", [TerminalOutcome.GATED, TerminalOutcome.DEGRADED_UNCOMMITTED,
    TerminalOutcome.FAILED_RECOVERABLE, TerminalOutcome.FAILED_TERMINAL, TerminalOutcome.ABANDONED])
def test_non_success_never_activates_snapshot(tmp_path, outcome):
    runtime, lease, handle, cohort, bundle = fixture(tmp_path)
    with pytest.raises(BundleRejected):
        runtime.commit_bundle(lease, handle, bundle, cohorts=(cohort,), outcome=outcome)
    assert runtime.status("demo").active_snapshot is None
    assert runtime.certificate_chain("demo") == ()


def test_replay_cannot_weaken_policy_or_change_outcome(tmp_path):
    runtime, lease, handle, cohort, bundle = fixture(tmp_path)
    cert = runtime.commit_bundle(lease, handle, bundle, cohorts=(cohort,),
        outcome=TerminalOutcome.COMPLETED, required_proof_types=("structural",))
    assert runtime.commit_bundle(lease, handle, bundle, cohorts=(cohort,),
        outcome=TerminalOutcome.COMPLETED, required_proof_types=("structural",)) == cert
    for overrides in [dict(required_proof_types=()), dict(outcome=TerminalOutcome.NO_CHANGE), dict(cohorts=())]:
        kwargs = dict(cohorts=(cohort,), outcome=TerminalOutcome.COMPLETED, required_proof_types=("structural",))
        kwargs.update(overrides)
        with pytest.raises(AlreadyCommitted):
            runtime.commit_bundle(lease, handle, bundle, **kwargs)


def test_canonical_keys_cannot_collide():
    with pytest.raises(TypeError):
        canonical_digest({1: "first", "1": "second"})
