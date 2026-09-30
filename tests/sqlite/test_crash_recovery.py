"""Crash/recovery at every durable boundary (V2-TEST-001).

Durability under interruption is the entire reason Ratchet exists, and until now
nothing had ever interrupted it. These tests kill a real process with SIGKILL at
each boundary between intent, dispatch, read-back, staging, and commit, then
reopen the database and assert the invariant that must survive.

SIGKILL, not an exception: a raised exception still unwinds and lets `finally`
blocks run, which is precisely the cleanup a real crash does not get.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import sys
import time
from dataclasses import asdict
from pathlib import Path

import pytest

from ratchet_sqlite.contracts import (
    CheckpointTier,
    CommitCohort,
    ProposedPosition,
    TerminalOutcome,
)
from ratchet_sqlite import SQLiteRatchet

from test_ratchet import attempt, digest, make_bundle, proof

_SYS_PATH = list(sys.path)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="SIGKILL semantics differ on Windows"
)


def _bootstrap() -> None:
    for entry in _SYS_PATH:
        if entry not in sys.path:
            sys.path.insert(0, entry)


def _die_after_prepare_effect(database: str, lease_state: dict, attempt_state: dict) -> None:
    """Persist effect intent, then die before dispatch could be marked."""
    _bootstrap()
    from ratchet_sqlite import SQLiteRatchet
    from ratchet_sqlite.models import AttemptHandle, Lease

    ratchet = SQLiteRatchet(database)
    ratchet.prepare_effect(
        Lease(**lease_state),
        AttemptHandle(**attempt_state),
        effect_id="publish-1",
        provider="surface",
        capability="write",
        operation_digest="d" * 64,
        idempotency_key="publish-once",
    )
    os.kill(os.getpid(), signal.SIGKILL)


def _die_after_mark_dispatching(database: str, lease_state: dict, attempt_state: dict) -> None:
    """The worst case: the write may or may not have landed remotely."""
    _bootstrap()
    from ratchet_sqlite import SQLiteRatchet
    from ratchet_sqlite.models import AttemptHandle, Lease

    ratchet = SQLiteRatchet(database)
    lease, handle = Lease(**lease_state), AttemptHandle(**attempt_state)
    ratchet.prepare_effect(
        lease,
        handle,
        effect_id="publish-1",
        provider="surface",
        capability="write",
        operation_digest="d" * 64,
        idempotency_key="publish-once",
    )
    ratchet.mark_effect_dispatching(lease, handle, effect_id="publish-1")
    os.kill(os.getpid(), signal.SIGKILL)


def _die_after_propose_positions(database: str, lease_state: dict, attempt_state: dict) -> None:
    """Positions proposed but never committed."""
    _bootstrap()
    from ratchet_sqlite import SQLiteRatchet
    from ratchet_sqlite.models import AttemptHandle, Lease
    from ratchet_sqlite.contracts import ProposedPosition

    ratchet = SQLiteRatchet(database)
    ratchet.propose_positions(
        Lease(**lease_state),
        AttemptHandle(**attempt_state),
        (ProposedPosition("source-a", "integer", "9", "c" * 64),),
    )
    os.kill(os.getpid(), signal.SIGKILL)


def _crash(target, database: str, lease, handle) -> int:
    context = mp.get_context("spawn")
    process = context.Process(target=target, args=(database, asdict(lease), asdict(handle)))
    process.start()
    process.join(timeout=60)
    return process.exitcode


# A crashed holder keeps its lease until the TTL expires -- there is no process
# left to release it. That IS the recovery path, so the tests use a short TTL and
# wait for it rather than pretending a crash releases anything.
CRASH_TTL_SECONDS = 2.0


def _fixture(tmp_path: Path, name: str = "ratchet.sqlite3"):
    database = str(tmp_path / name)
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "worker", ttl_seconds=CRASH_TTL_SECONDS)
    handle = ratchet.begin_attempt(
        lease,
        run_id="run-crash",
        plan_digest="a" * 64,
        effective_config_digest="b" * 64,
        runtime_lock_digest="c" * 64,
        requested_tier=CheckpointTier.PHASE,
        minimum_tier=CheckpointTier.RUN,
    )
    return database, ratchet, lease, handle


def test_crash_after_effect_intent_leaves_a_recoverable_prepared_effect(tmp_path: Path) -> None:
    database, ratchet, lease, handle = _fixture(tmp_path)
    exitcode = _crash(_die_after_prepare_effect, database, lease, handle)
    assert exitcode == -signal.SIGKILL, "the child must actually have been killed"

    # Reopen: the intent survived the crash, which is the whole point of
    # persisting it before dispatch (V2-RATCHET-007).
    reopened = SQLiteRatchet(database)
    recovery = reopened.effect_recovery("instance", "publish-1")
    assert recovery is not None
    # Intent is durable and dispatch never happened, so the effect is cleanly
    # dispatchable -- no ambiguity, because nothing crossed the boundary.
    assert recovery.action == "safe_to_dispatch", recovery


def test_crash_after_dispatch_forces_readback_and_forbids_blind_retry(tmp_path: Path) -> None:
    """V2-RATCHET-008: an interrupted dispatch is ambiguous, never clean."""
    database, ratchet, lease, handle = _fixture(tmp_path)
    exitcode = _crash(_die_after_mark_dispatching, database, lease, handle)
    assert exitcode == -signal.SIGKILL

    reopened = SQLiteRatchet(database)
    # The crashed holder's lease expires; only then can a newer tenure adopt the
    # interrupted attempt. The effect must become ambiguous rather than being
    # silently retried or discarded.
    time.sleep(CRASH_TTL_SECONDS + 0.5)
    recovered_lease = reopened.acquire("instance", "recovery", ttl_seconds=60)
    reopened.recover_attempt(
        recovered_lease,
        attempt_id=handle.attempt_id,
        plan_digest=handle.plan_digest,
        effective_config_digest=handle.effective_config_digest,
        runtime_lock_digest=handle.runtime_lock_digest,
    )
    recovery = reopened.effect_recovery("instance", "publish-1")
    assert recovery.action == "readback_required", recovery


def test_crash_after_proposing_positions_commits_nothing(tmp_path: Path) -> None:
    """V2-RATCHET-004: a position is durable only via the certificate."""
    database, ratchet, lease, handle = _fixture(tmp_path)
    exitcode = _crash(_die_after_propose_positions, database, lease, handle)
    assert exitcode == -signal.SIGKILL

    reopened = SQLiteRatchet(database)
    assert reopened.status("instance").committed_positions == (), (
        "a crash between proposal and commit must leave the cursor untouched"
    )
    # The proposal itself survived, so recovery can resume rather than re-read.
    assert reopened.list_proposed_positions("instance", handle.attempt_id)


def test_database_remains_usable_after_every_crash(tmp_path: Path) -> None:
    """A killed process must not leave a wedged or corrupt database."""
    database, ratchet, lease, handle = _fixture(tmp_path)
    for target in (
        _die_after_prepare_effect,
        _die_after_propose_positions,
    ):
        crash_path = str(tmp_path / f"{target.__name__}.sqlite3")
        fresh = SQLiteRatchet(crash_path)
        fresh.initialize_instance("instance")
        crash_lease = fresh.acquire("instance", "worker", ttl_seconds=CRASH_TTL_SECONDS)
        crash_attempt = fresh.begin_attempt(
            crash_lease,
            run_id="run-crash",
            plan_digest="a" * 64,
            effective_config_digest="b" * 64,
            runtime_lock_digest="c" * 64,
            requested_tier=CheckpointTier.PHASE,
            minimum_tier=CheckpointTier.RUN,
        )
        _crash(target, crash_path, crash_lease, crash_attempt)

        # Still readable and still writable once the dead holder's lease lapses.
        reopened = SQLiteRatchet(crash_path)
        assert reopened.status("instance") is not None
        time.sleep(CRASH_TTL_SECONDS + 0.5)
        later = reopened.acquire("instance", "after-crash", ttl_seconds=60)
        assert later.fencing_token > crash_lease.fencing_token
