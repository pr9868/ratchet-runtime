"""Real multi-process fencing and duplicate-dispatch tests (V2-TEST-006).

Every other Ratchet test runs single-process with an injected clock, so the
multi-writer claims in V2-RATCHET-002 and V2-RATCHET-007 were unverified by
construction. These use actual concurrent OS processes against one SQLite file,
which is the situation the fencing machinery exists for.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import sys
from pathlib import Path

import pytest

from ratchet_sqlite import SQLiteRatchet

# Child processes must import the packages the parent has on sys.path.
_SYS_PATH = list(sys.path)


def _bootstrap() -> None:
    for entry in _SYS_PATH:
        if entry not in sys.path:
            sys.path.insert(0, entry)


def _acquire_worker(database: str, instance: str, worker: str, queue) -> None:
    _bootstrap()
    from ratchet_sqlite import SQLiteRatchet
    from ratchet_sqlite.errors import LeaseBusy

    try:
        lease = SQLiteRatchet(database).acquire(instance, worker, ttl_seconds=30)
        queue.put(("acquired", worker, lease.fencing_token))
    except LeaseBusy:
        queue.put(("busy", worker, None))
    except Exception as error:  # pragma: no cover - surfaced as a test failure
        queue.put(("error", worker, repr(error)))


def _init_worker(database: str, instance: str, queue) -> None:
    _bootstrap()
    from ratchet_sqlite import SQLiteRatchet

    try:
        SQLiteRatchet(database).initialize_instance(instance)
        queue.put(("ok", None))
    except Exception as error:
        queue.put(("error", repr(error)))


def _prepare_effect_worker(
    database: str, instance: str, lease_state: dict, attempt_state: dict, worker: str, queue
) -> None:
    """Prepare an effect under a lease the PARENT already holds.

    Each child must share one tenure. If a child acquired its own lease, lease
    exclusivity alone would guarantee at most one preparer and the
    `UNIQUE(instance_id, idempotency_key)` constraint -- the thing under test --
    would never be reached.
    """
    _bootstrap()
    from ratchet_sqlite import SQLiteRatchet
    from ratchet_sqlite.models import AttemptHandle, Lease

    try:
        ratchet = SQLiteRatchet(database)
        lease = Lease(**lease_state)
        attempt = AttemptHandle(**attempt_state)
        ratchet.prepare_effect(
            lease,
            attempt,
            # Distinct effect ids, ONE shared idempotency key: the key is what
            # must prevent a second durable intent.
            effect_id=f"effect-{worker}",
            provider="surface",
            capability="write",
            operation_digest="d" * 64,
            idempotency_key="publish-once",
        )
        queue.put(("prepared", worker))
    except Exception as error:
        queue.put(("rejected", worker, type(error).__name__))


def _run(target, args, count: int) -> list:
    context = mp.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=target, args=(*args, f"worker-{index}", queue))
        for index in range(count)
    ]
    for process in processes:
        process.start()
    # Drain by expected count with a timeout rather than polling `empty()`,
    # which races with child teardown and silently loses results.
    results = []
    for _ in range(count):
        try:
            results.append(queue.get(timeout=60))
        except Exception:
            break
    for process in processes:
        process.join(timeout=60)
    return results


pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="spawn semantics differ on Windows"
)


def test_only_one_process_holds_the_lease(tmp_path: Path) -> None:
    """V2-RATCHET-002 against real concurrency, not an injected clock."""
    database = str(tmp_path / "ratchet.sqlite3")
    SQLiteRatchet(database).initialize_instance("instance")

    results = _run(_acquire_worker, (database, "instance"), count=6)
    acquired = [item for item in results if item[0] == "acquired"]
    busy = [item for item in results if item[0] == "busy"]
    errors = [item for item in results if item[0] == "error"]

    assert not errors, errors
    assert len(acquired) == 1, f"expected exactly one holder, got {acquired}"
    assert len(busy) == 5
    # The winner's fencing token is the instance's first.
    assert acquired[0][2] == 1


def test_concurrent_instance_initialization_does_not_corrupt(tmp_path: Path) -> None:
    """Schema creation raced from several processes must not leave a broken file."""
    database = str(tmp_path / "ratchet.sqlite3")
    context = mp.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_init_worker, args=(database, "instance", queue))
        for _ in range(6)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=60)

    # Whatever the races did, the database must still be usable afterwards.
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "after", ttl_seconds=30)
    assert lease.fencing_token >= 1


def test_duplicate_idempotency_key_cannot_be_prepared_twice(tmp_path: Path) -> None:
    """V2-RATCHET-007 under real contention: one effect intent, not two.

    All four children share the parent's lease and attempt, so the only thing
    standing between them and two durable dispatch intents is the idempotency
    key uniqueness constraint.
    """
    from dataclasses import asdict

    from ratchet_sqlite.contracts import CheckpointTier

    database = str(tmp_path / "ratchet.sqlite3")
    ratchet = SQLiteRatchet(database)
    ratchet.initialize_instance("instance")
    lease = ratchet.acquire("instance", "parent", ttl_seconds=120)
    attempt = ratchet.begin_attempt(
        lease,
        run_id="run-shared",
        plan_digest="a" * 64,
        effective_config_digest="b" * 64,
        runtime_lock_digest="c" * 64,
        requested_tier=CheckpointTier.PHASE,
        minimum_tier=CheckpointTier.RUN,
    )

    results = _run(
        _prepare_effect_worker,
        (database, "instance", asdict(lease), asdict(attempt)),
        count=4,
    )
    prepared = [item for item in results if item[0] == "prepared"]
    rejected = [item for item in results if item[0] == "rejected"]

    assert len(results) == 4, f"lost a worker result: {results}"
    # Exactly one -- not "at most one", which would also pass if every child
    # failed for an unrelated reason such as an import error.
    assert len(prepared) == 1, f"expected exactly one durable intent, got {results}"
    assert len(rejected) == 3
