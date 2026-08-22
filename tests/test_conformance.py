"""
Ratchet Runtime conformance suite.

Every guarantee in docs/CONTRACT.md v0.5.0-alpha.2, asserted WITHOUT invoking an agent.
That property is the point: if a guarantee needed an LLM to test, it would not be a
guarantee — it would be a hope.

Run:  python3 tests/test_conformance.py
"""

import os
import shutil
import sys
import tempfile
import time
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from ratchet_runtime import (  # noqa: E402
    Budget, CircuitOpen, EffectState, RatchetError, Reconciliation, Runner, StateStore,
    Step as RuntimeStep, Tenure, TenureLost, TenureUnavailable, atomic_write, read_json,
    staleness_check,
)

RESULTS = []


def test(name):
    def deco(fn):
        RESULTS.append((name, fn))
        return fn
    return deco


def ok(**kw):
    d = {"ok": True}
    d.update(kw)
    return d


def Step(name, *args, **kwargs):
    """Keep unrelated tests terse while the public API still requires real declarations."""
    kwargs.setdefault("postcondition", lambda _ctx, _result: True)
    if kwargs.get("side_effecting") and "idempotency_key" not in kwargs:
        kwargs["idempotency_key"] = lambda _ctx, n=name: f"test:{n}"
    return RuntimeStep(name, *args, **kwargs)


def tmpstore():
    return StateStore(tempfile.mkdtemp(prefix="ratchet-test-"))


def _contend(root, run_id, q):
    """Module-level so it survives a spawn start method, not just fork."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
    from ratchet_runtime import RatchetError, StateStore as SS, Tenure as T, TenureUnavailable as TU
    t = T(SS(root), heartbeat_period=60)
    try:
        rec = t.acquire(run_id)
        q.put(("won", run_id, rec.tenure))
        time.sleep(0.5)                      # hold it until every racer has decided
    except TU:
        q.put(("lost", run_id, None))
    except RatchetError as e:
        q.put(("corrupt", run_id, str(e)))   # the D-2 empty-lock window, if still open
    except Exception as e:                   # noqa: BLE001
        q.put(("error", run_id, repr(e)))


# ═════════════════════════════════════════════════════════════════════════════
# G1 · Ordering
# ═════════════════════════════════════════════════════════════════════════════

@test("G1: a step after a failure never executes")
def _():
    store = tmpstore()
    ran = []
    steps = [
        Step("a", invoke=lambda c: (ran.append("a"), ok())[1]),
        Step("b", invoke=lambda c: (ran.append("b"), {"ok": False, "diagnostics": ["boom"]})[1]),
        Step("c", invoke=lambda c: (ran.append("c"), ok())[1], depends_on=["b"]),
    ]
    out = Runner(store, steps).run()
    assert ran == ["a", "b"], ran
    assert not out.completed
    assert out.failed_step == "b"


@test("G1: a declared dependency on a later step is rejected at construction")
def _():
    store = tmpstore()
    try:
        Runner(store, [Step("a", invoke=lambda c: ok(), depends_on=["z"])])
        raise AssertionError("should have rejected unknown dependency")
    except Exception as e:
        assert "unknown step" in str(e), e


# ═════════════════════════════════════════════════════════════════════════════
# G2 · Exclusive write tenure
# ═════════════════════════════════════════════════════════════════════════════

@test("G2: concurrent acquisition yields exactly one winner")
def _():
    store = tmpstore()
    a, b = Tenure(store, heartbeat_period=60), Tenure(store, heartbeat_period=60)
    a.acquire("run-a")
    try:
        b.acquire("run-b")
        raise AssertionError("second acquisition should have failed")
    except TenureUnavailable:
        pass


@test("G2: a lock past TTL is broken, and the break is recorded")
def _():
    store = tmpstore()
    # clock_skew_allowance=0: this test is single-host, so there is no skew to allow for.
    # In production it is 30s and deliberately biases toward NOT breaking a live lock.
    a = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    a.acquire("run-a")
    time.sleep(0.25)
    b = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    b.acquire("run-b")
    assert b.breaks == ["run-a"], b.breaks


@test("G2: a live lock is NOT broken merely because the run is long")
def _():
    # H-1: the v0.1 TTL derived from run duration, so the busy-day run — the one that
    # matters — got its lock broken. TTL now derives from heartbeat period, so a run may
    # take arbitrarily long provided it keeps beating.
    store = tmpstore()
    a = Tenure(store, heartbeat_period=1, ttl_multiple=5, clock_skew_allowance=0)
    a.acquire("run-a")
    # Make the run itself a day old, then publish a fresh heartbeat. This proves tenure
    # age is governed by heartbeat freshness rather than run duration without making the
    # assertion depend on scheduler or fsync latency on a busy CI worker.
    a.held.started_at -= 86_400
    a.heartbeat()
    b = Tenure(store, heartbeat_period=1, ttl_multiple=5, clock_skew_allowance=0)
    try:
        b.acquire("run-b")
        raise AssertionError("a heartbeating run's lock must not be broken")
    except TenureUnavailable:
        pass
    assert b.breaks == [], b.breaks


@test("G2: acquisition is atomic under a REAL multi-process race")
def _():
    # The sequential two-object test proves the guard exists; it cannot prove atomicity.
    # C-4 was a spec defect of exactly that shape, so fork real processes and race them.
    import multiprocessing as mp

    store = tmpstore()
    q = mp.Queue()
    procs = [mp.Process(target=_contend, args=(store.root, f"run-{i}", q)) for i in range(8)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(20)

    outcomes = [q.get() for _ in range(8)]
    kinds = [o[0] for o in outcomes]
    assert "corrupt" not in kinds, f"empty-lock window still open: {outcomes}"
    assert "error" not in kinds, f"unexpected failure: {outcomes}"
    assert kinds.count("won") == 1, f"expected exactly one winner, got {outcomes}"
    winner = next(o for o in outcomes if o[0] == "won")
    assert winner[2] is not None, "winner must have an allocated tenure number"


@test("G2: concurrent stale-lock breakers yield one winner and one new fencing token")
def _():
    import multiprocessing as mp

    store = tmpstore()
    old = Tenure(store, heartbeat_period=0.01, ttl_multiple=1, clock_skew_allowance=0)
    old.acquire("old-run")
    old.held.heartbeat_at = 0
    old._write(old.held)

    q = mp.Queue()
    procs = [mp.Process(target=_contend, args=(store.root, f"breaker-{i}", q)) for i in range(8)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(20)

    outcomes = [q.get() for _ in range(8)]
    winners = [o for o in outcomes if o[0] == "won"]
    assert len(winners) == 1, outcomes
    assert winners[0][2] == 2, outcomes


@test("G2: a run that LOSES tenure refuses to write — the clause that makes G2 true")
def _():
    store = tmpstore()
    a = Tenure(store, heartbeat_period=60)
    a.acquire("run-a")
    # Simulate a break by another run while A is stalled.
    os.unlink(a.path)
    b = Tenure(store, heartbeat_period=60)
    b.acquire("run-b")
    try:
        a.assert_held()
        raise AssertionError("dispossessed run must not be allowed to write")
    except TenureLost:
        pass


@test("G2: Runner persists nothing after tenure loss")
def _():
    import json

    store = tmpstore()
    box = {}

    def displaced(_ctx):
        held = box["runner"].tenure.held
        replacement = {**held.__dict__, "run_id": "new-owner", "tenure": held.tenure + 1}
        atomic_write(store.p("lock.json"), json.dumps(replacement))
        return ok()

    runner = Runner(store, [Step("displaced", invoke=displaced)], heartbeat_period=60)
    box["runner"] = runner
    out = runner.run()
    assert "tenure lost" in (out.reason or "")
    assert read_json(store.p("breaker.json")) is None
    assert read_json(store.p("runs", f"{out.run_id}.json")) is None


@test("G2: Runner heartbeats while a long step is executing")
def _():
    store = tmpstore()
    blocked = []

    def long_step(_ctx):
        time.sleep(0.15)
        contender = Tenure(store, heartbeat_period=0.02, ttl_multiple=5,
                            clock_skew_allowance=0)
        try:
            contender.acquire("contender")
        except TenureUnavailable:
            blocked.append(True)
        return ok()

    out = Runner(store, [Step("long", invoke=long_step)], heartbeat_period=0.02).run()
    assert out.completed and blocked == [True]


@test("G2: release is ownership-checked — a non-owner cannot release")
def _():
    store = tmpstore()
    a = Tenure(store, heartbeat_period=60)
    a.acquire("run-a")
    impostor = Tenure(store, heartbeat_period=60)
    impostor.held = a.held.__class__(**{**a.held.__dict__, "run_id": "run-x", "tenure": 999})
    impostor.release()
    assert os.path.exists(a.path), "impostor released a lock it did not own"
    a.release()
    assert not os.path.exists(a.path)


@test("G2: tenure counter is monotonic across breaks (fencing input)")
def _():
    store = tmpstore()
    seen = []
    for i in range(3):
        t = Tenure(store, heartbeat_period=0.01, ttl_multiple=1, clock_skew_allowance=0)
        time.sleep(0.05)
        seen.append(t.acquire(f"run-{i}").tenure)
    assert seen == sorted(seen) and len(set(seen)) == 3, seen


# ═════════════════════════════════════════════════════════════════════════════
# G4 · Verified completion — the one v0.1 got wrong
# ═════════════════════════════════════════════════════════════════════════════

@test("G4: a step without a runner-evaluated postcondition is rejected")
def _():
    try:
        RuntimeStep("unverified", invoke=lambda _ctx: ok())
        raise AssertionError("step without a postcondition was accepted")
    except RatchetError as e:
        assert "postcondition" in str(e)


@test("G4: NO completion marker is written when a step fails")
def _():
    store = tmpstore()
    steps = [Step("a", invoke=lambda c: {"ok": False, "diagnostics": ["x"]})]
    out = Runner(store, steps).run()
    assert not out.completed
    assert store.completion() is None, "marker landed over a failure"


@test("G4: ok=true with a FAILING post-condition is a failed step")
def _():
    # This is the finding that made v0.1's G4 false. The agent lies (or is simply
    # wrong); the runner checks the effect itself and refuses completion.
    store = tmpstore()
    artifact = store.p("artifact.txt")
    steps = [Step(
        "writes-a-file",
        invoke=lambda c: ok(note="I definitely wrote it"),   # agent self-reports success
        postcondition=lambda c, r: os.path.exists(artifact),  # it did not
    )]
    out = Runner(store, steps).run()
    assert not out.completed, "agent self-report was accepted as sufficient"
    assert out.steps.get("writes-a-file") == "postcondition-failed"
    assert store.completion() is None


@test("G4: a passing post-condition completes")
def _():
    store = tmpstore()
    artifact = store.p("artifact.txt")

    def do(c):
        atomic_write(artifact, "content")
        return ok()

    steps = [Step("writes-a-file", invoke=do,
                  postcondition=lambda c, r: os.path.exists(artifact))]
    out = Runner(store, steps).run()
    assert out.completed, out.reason
    assert store.completion() is not None


@test("G4: a post-condition that RAISES is a failure, not a pass")
def _():
    store = tmpstore()
    steps = [Step("s", invoke=lambda c: ok(),
                  postcondition=lambda c, r: (_ for _ in ()).throw(RuntimeError("nope")))]
    out = Runner(store, steps).run()
    assert not out.completed


@test("G4: an invalid step result is a failure — ambiguity is failure")
def _():
    store = tmpstore()
    for bad in ["not a dict", {}, {"status": "fine"}, None]:
        s = tmpstore()
        out = Runner(s, [Step("s", invoke=lambda c, b=bad: b)]).run()
        assert not out.completed, f"accepted {bad!r}"


@test("G4/G3: a verified pause is neutral and commits neither marker nor positions")
def _():
    store = tmpstore()
    ran = []
    # Establish a real failure streak. A normal approval pause must neither erase it nor
    # increment it toward circuit-open.
    for _n in range(2):
        Runner(store, [Step("fails", invoke=lambda c: {"ok": False})],
               budget=Budget(max_consecutive_failures=3)).run()
    breaker_before = read_json(store.p("breaker.json"))

    steps = [
        Step("preview", invoke=lambda c: {
            "ok": True, "paused": "approval_pending", "positions": {"src": "p1"}},
            postcondition=lambda c, r: (ran.append("verified"), True)[1]),
        Step("after", invoke=lambda c: (ran.append("after"), ok())[1],
             depends_on=["preview"]),
    ]
    out = Runner(store, steps, budget=Budget(max_consecutive_failures=3)).run()
    assert out.paused_at == "preview" and not out.completed
    assert ran == ["verified"], "pause post-condition must run; later steps must not"
    assert store.completion() is None
    assert store.positions() == {}, "preview coverage is pending, not consumed"
    assert read_json(store.p("breaker.json")) == breaker_before
    assert read_json(store.p("intents", "preview.json")) is None


# ═════════════════════════════════════════════════════════════════════════════
# G3 · Position integrity
# ═════════════════════════════════════════════════════════════════════════════

@test("G3: positions do NOT advance when the run fails")
def _():
    store = tmpstore()
    steps = [
        Step("gather", invoke=lambda c: ok(positions={"src-a": "100"})),
        Step("fail", invoke=lambda c: {"ok": False}),
    ]
    Runner(store, steps).run()
    assert store.positions() == {}, store.positions()


@test("G3: a failing source keeps its position while others advance")
def _():
    store = tmpstore()
    r1 = Runner(store, [Step("g", invoke=lambda c: ok(positions={"src-a": "10", "src-b": "10"}))])
    assert r1.run().completed

    # src-b fails: it reports no new position, so it must retain "10".
    def gather(c):
        return ok(positions={"src-a": "20"}, degraded=["src-b"])

    r2 = Runner(store, [Step("g", invoke=gather)])
    out = r2.run()
    assert out.completed
    assert out.degraded == ["src-b"]
    assert store.positions() == {"src-a": "20", "src-b": "10"}, store.positions()


@test("G3: positions are committed atomically WITH the marker")
def _():
    store = tmpstore()
    Runner(store, [Step("g", invoke=lambda c: ok(positions={"s": "7"}))]).run()
    c = store.completion()
    assert c["positions"] == {"s": "7"}
    assert c["run_id"] and c["completed_at"]
    # one file — a marker cannot exist without its positions
    assert read_json(store.p("completion.json"))["positions"]["s"] == "7"


# ═════════════════════════════════════════════════════════════════════════════
# G5 · Effect integrity
# ═════════════════════════════════════════════════════════════════════════════

@test("G5: a crashed step leaves an intent record")
def _():
    store = tmpstore()
    external = store.p("external.txt")

    def crash(c):
        atomic_write(external, "written")   # external effect lands
        raise RuntimeError("died before reporting")

    Runner(store, [Step("w", invoke=crash, side_effecting=True,
                        idempotency_key=lambda c: "key-1")]).run()
    intent = read_json(store.p("intents", "w.json"))
    assert intent and intent["idempotency_key"] == "key-1", intent


@test("G5: a side-effecting step without an idempotency key is rejected")
def _():
    try:
        RuntimeStep("write", invoke=lambda _ctx: ok(),
                    postcondition=lambda _ctx, _result: True, side_effecting=True)
        raise AssertionError("side effect without idempotency was accepted")
    except RatchetError as e:
        assert "idempotency" in str(e)


@test("G5: an orphan without reconciliation blocks instead of blindly retrying")
def _():
    store = tmpstore()
    calls = []

    def crash(_ctx):
        raise RuntimeError("died after an unknown effect")

    key = lambda _ctx: "stable-key"  # noqa: E731
    Runner(store, [Step("w", invoke=crash, side_effecting=True,
                        idempotency_key=key)]).run()
    out = Runner(store, [Step("w", invoke=lambda _ctx: (calls.append("ran"), ok())[1],
                              side_effecting=True, idempotency_key=key)]).run()
    assert not out.completed
    assert calls == []
    assert "requires reconciliation" in (out.reason or "")


@test("G5: an orphan intent is RECONCILED, not blindly re-run")
def _():
    store = tmpstore()
    calls = []
    external = store.p("external.txt")

    def crash(c):
        atomic_write(external, "written once")
        raise RuntimeError("died before reporting")

    def do(c):
        calls.append("ran")
        atomic_write(external, "written twice")
        return ok()

    key = lambda c: "key-1"                                    # noqa: E731
    recon = lambda c, intent: Reconciliation(                    # noqa: E731
        EffectState.PRESENT if os.path.exists(external) else EffectState.ABSENT)

    Runner(store, [Step("w", invoke=crash, side_effecting=True, idempotency_key=key)]).run()
    out = Runner(store, [Step("w", invoke=do, side_effecting=True,
                              idempotency_key=key, reconcile=recon)]).run()
    assert out.completed, out.reason
    assert calls == [], "step re-ran despite the effect having landed — duplicate write"
    assert "w" in out.reconciled
    assert open(external).read() == "written once"


@test("G5: an at_most_once step with an unreconciled orphan FAILS rather than retrying")
def _():
    store = tmpstore()

    def crash(c):
        raise RuntimeError("died")

    Runner(store, [Step("w", invoke=crash, side_effecting=True,
                        idempotency_key=lambda c: "k")]).run()
    out = Runner(store, [Step("w", invoke=lambda c: ok(), side_effecting=True,
                              at_most_once=True, idempotency_key=lambda c: "k")]).run()
    assert not out.completed
    assert "at_most_once" in (out.reason or "")


# ═════════════════════════════════════════════════════════════════════════════
# G6 · State isolation and durability
# ═════════════════════════════════════════════════════════════════════════════

@test("G6: runner state directory is not world/group accessible")
def _():
    store = tmpstore()
    mode = os.stat(store.root).st_mode & 0o777
    assert mode == 0o700, oct(mode)


@test("G6: atomic_write leaves no partial file and no temp residue")
def _():
    store = tmpstore()
    p = store.p("x.json")
    atomic_write(p, '{"a":1}')
    atomic_write(p, '{"a":2}')
    assert read_json(p) == {"a": 2}
    leftovers = [f for f in os.listdir(store.root) if f.startswith(".") and f.endswith(".tmp")]
    assert not leftovers, leftovers


@test("G6: a corrupt state file is an ERROR, never silently treated as absent")
def _():
    store = tmpstore()
    with open(store.p("completion.json"), "w") as f:
        f.write("{ this is not json")
    try:
        store.completion()
        raise AssertionError("corruption was silently swallowed")
    except Exception as e:
        assert "corrupt" in str(e), e


# ═════════════════════════════════════════════════════════════════════════════
# §10 · Boundedness
# ═════════════════════════════════════════════════════════════════════════════

@test("§10: circuit breaker opens after N consecutive failures and needs explicit reset")
def _():
    store = tmpstore()
    steps = [Step("s", invoke=lambda c: {"ok": False})]
    b = Budget(max_consecutive_failures=2)
    Runner(store, steps, budget=b).run()
    Runner(store, steps, budget=b).run()
    try:
        Runner(store, steps, budget=b).run()
        raise AssertionError("breaker did not open")
    except CircuitOpen:
        pass
    r = Runner(store, [Step("s", invoke=lambda c: ok())], budget=b)
    r.reset_breaker()
    assert r.run().completed


@test("§10: a successful run clears the failure count")
def _():
    store = tmpstore()
    b = Budget(max_consecutive_failures=2)
    Runner(store, [Step("s", invoke=lambda c: {"ok": False})], budget=b).run()
    Runner(store, [Step("s", invoke=lambda c: ok())], budget=b).run()
    assert read_json(store.p("breaker.json"))["consecutive_failures"] == 0


@test("§10: max_steps is enforced")
def _():
    store = tmpstore()
    steps = [Step(f"s{i}", invoke=lambda c: ok()) for i in range(5)]
    out = Runner(store, steps, budget=Budget(max_steps=3)).run()
    assert not out.completed
    assert "max_steps" in (out.reason or "")


@test("§10: an over-budget step cannot commit completion")
def _():
    store = tmpstore()

    def slow(_ctx):
        time.sleep(0.03)
        return ok()

    out = Runner(store, [Step("slow", invoke=slow)],
                 budget=Budget(max_seconds=0.01), heartbeat_period=0.005).run()
    assert not out.completed
    assert store.completion() is None
    assert "max_seconds" in (out.reason or "")


# ═════════════════════════════════════════════════════════════════════════════
# §11 · Observability
# ═════════════════════════════════════════════════════════════════════════════

@test("§11: watchdog detects a workflow that has never completed")
def _():
    assert staleness_check(tmpstore(), 60) is not None


@test("§11: watchdog is satisfied by a fresh completion")
def _():
    store = tmpstore()
    Runner(store, [Step("s", invoke=lambda c: ok())]).run()
    assert staleness_check(store, 3600) is None


@test("§11: every run writes a run record, success or failure")
def _():
    store = tmpstore()
    a = Runner(store, [Step("s", invoke=lambda c: ok())]).run()
    b = Runner(store, [Step("s", invoke=lambda c: {"ok": False})]).run()
    for out in (a, b):
        rec = read_json(store.p("runs", f"{out.run_id}.json"))
        assert rec and rec["run_id"] == out.run_id
        assert rec["completed"] is out.completed


# ═════════════════════════════════════════════════════════════════════════════
# Integration
# ═════════════════════════════════════════════════════════════════════════════

@test("integration: failure mid-sequence leaves NO marker, NO positions, intact prior state")
def _():
    store = tmpstore()
    assert Runner(store, [Step("g", invoke=lambda c: ok(positions={"s": "1"}))]).run().completed
    before = store.completion()

    steps = [
        Step("g", invoke=lambda c: ok(positions={"s": "2"})),
        Step("w", invoke=lambda c: ok(), postcondition=lambda c, r: False),
    ]
    out = Runner(store, steps).run()
    assert not out.completed
    assert store.completion() == before, "failed run mutated committed state"
    assert store.positions() == {"s": "1"}


def main():
    passed, failed = 0, []
    for name, fn in RESULTS:
        try:
            fn()
            passed += 1
            print(f"  PASS  {name}")
        except Exception:
            failed.append((name, traceback.format_exc()))
            print(f"  FAIL  {name}")
    print(f"\n{passed}/{len(RESULTS)} passed")
    for name, tb in failed:
        print(f"\n--- {name} ---\n{tb}")
    return 1 if failed else 0




@test("G2: a lock that cannot be UNLINKED is still seizable by atomic overwrite")
def _():
    # Some filesystems permit create and rename but deny unlink. Under an
    # unlink-then-reacquire design a stale lock could never be released there.
    store = tmpstore()
    a = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    a.acquire("run-a")
    time.sleep(0.25)

    real_unlink = os.unlink

    def deny(path, *args, **kw):
        if path.endswith("lock.json"):
            raise PermissionError(1, "Operation not permitted", path)
        return real_unlink(path, *args, **kw)

    os.unlink = deny
    try:
        b = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
        rec = b.acquire("run-b")
        assert rec.run_id == "run-b"
        assert b.breaks == ["run-a"], b.breaks
    finally:
        os.unlink = real_unlink


@test("G2: seizing a stale lock leaves NO window in which the lock is absent")
def _():
    store = tmpstore()
    a = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    a.acquire("run-a")
    time.sleep(0.25)
    b = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    b.acquire("run-b")
    assert os.path.exists(b.path), (
        "unlink-then-recreate opens a gap every waiting run races into")
    assert b._read().run_id == "run-b"


@test("G2: a CORRUPT lock is seizable and the seizure is recorded")
def _():
    # Narrow exception to §8. The lock carries no history — unreadable, it asserts nothing,
    # and refusing to proceed would let corrupt bookkeeping brick the workflow forever.
    store = tmpstore()
    a = Tenure(store, heartbeat_period=60)
    a.acquire("run-a")
    with open(a.path, "w") as f:
        f.write("}{ not json")
    b = Tenure(store, heartbeat_period=60)
    rec = b.acquire("run-b")
    assert rec.run_id == "run-b"
    assert any("corrupt" in x for x in b.breaks), b.breaks


@test("G6: a corrupt COMPLETION marker is still a hard error — the lock exception is narrow")
def _():
    store = tmpstore()
    store.commit_completion("run-a", {"s": "p1"}, {"ok": True})
    with open(store.p("completion.json"), "w") as f:
        f.write("}{ not json")
    try:
        store.completion()
        raise AssertionError("seizing this would discard real history")
    except RatchetError:
        pass


@test("G2: breaking a stale lock NEVER signals another process — pids are recycled")
def _():
    # The withdrawn behaviour killed by pid. A stale record names a process we believe is
    # dead, so the OS may have reassigned that number — signalling it kills a bystander,
    # silently. Observed: a stale lock made the runner SIGTERM the shell that invoked it.
    import ratchet_runtime as R
    store = tmpstore()
    a = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    rec = a.acquire("run-a")
    rec.pid = 999999                     # a pid we do not own
    a._write(rec)
    time.sleep(0.25)

    killed = []
    real_kill = os.kill
    os.kill = lambda pid, sig: killed.append((pid, sig))
    try:
        b = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
        b.acquire("run-b")
    finally:
        os.kill = real_kill
    assert killed == [], f"signalled a foreign pid: {killed}"
    assert R._terminate_if_reachable(rec) is False


@test("G2: fencing is what stops the dispossessed writer, not the kill")
def _():
    store = tmpstore()
    a = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    a.acquire("run-a")
    time.sleep(0.25)
    b = Tenure(store, heartbeat_period=0.05, ttl_multiple=2, clock_skew_allowance=0)
    b.acquire("run-b")
    try:
        a.assert_held()
        raise AssertionError("the dispossessed run must refuse to write")
    except TenureLost:
        pass


@test("G2: release SUCCEEDS where unlink is denied, by writing an expired record")
def _():
    store = tmpstore()
    a = Tenure(store, heartbeat_period=60)
    a.acquire("run-a")
    real_unlink = os.unlink

    def deny(path, *args, **kw):
        if path.endswith("lock.json"):
            raise PermissionError(1, "Operation not permitted", path)
        return real_unlink(path, *args, **kw)

    os.unlink = deny
    try:
        a.release()
    finally:
        os.unlink = real_unlink

    b = Tenure(store, heartbeat_period=60)
    b.acquire("run-b")           # must NOT raise: the released lock reads as expired
    assert b.held.run_id == "run-b"


@test("a release failure does not replace the run's real outcome")
def _():
    store = tmpstore()
    steps = [Step("a", invoke=lambda c: ok())]
    r = Runner(store, steps)
    r.tenure.release = lambda: (_ for _ in ()).throw(OSError("release exploded"))
    out = r.run()
    assert out.completed, "cleanup housekeeping must not turn a good run into a crash"


@test("G1/G6: a step name cannot escape the intent directory")
def _():
    for bad in ["../completion", "nested/step", "", "."]:
        try:
            RuntimeStep(bad, invoke=lambda _ctx: ok(),
                        postcondition=lambda _ctx, _result: True)
            raise AssertionError(f"unsafe step name was accepted: {bad!r}")
        except RatchetError:
            pass


@test("G2/G5: callbacks receive the fencing token and evaluated effect key, not the state store")
def _():
    store = tmpstore()
    observed = {}

    def invoke(ctx):
        observed.update(token=ctx.tenure_token, key=ctx.idempotency_key,
                        step=ctx.step_name, has_store=hasattr(ctx, "store"))
        return ok()

    out = Runner(store, [Step(
        "publish", invoke=invoke, side_effecting=True,
        idempotency_key=lambda _ctx: "publish:42",
    )]).run()
    assert out.completed
    assert observed == {"token": 1, "key": "publish:42", "step": "publish",
                        "has_store": False}


@test("G2: a failure after acquisition still stops heartbeat and releases tenure")
def _():
    store = tmpstore()
    atomic_write(store.p("completion.json"), "}{ corrupt")
    runner = Runner(store, [Step("s", invoke=lambda _ctx: ok())], heartbeat_period=0.01)
    try:
        runner.run()
        raise AssertionError("corrupt completion was accepted")
    except RatchetError:
        pass
    assert runner.tenure._heartbeat_thread is None
    successor = Tenure(store, heartbeat_period=60)
    successor.acquire("successor")
    successor.release()


@test("G2: an unexpected heartbeat error fails closed without persistent outcome writes")
def _():
    store = tmpstore()
    runner = Runner(
        store,
        [Step("slow", invoke=lambda _ctx: (time.sleep(0.04), ok())[1])],
        heartbeat_period=0.005,
    )
    real_heartbeat = runner.tenure.heartbeat
    calls = []

    def flaky_heartbeat():
        calls.append("beat")
        if len(calls) > 1:
            raise OSError("simulated heartbeat I/O failure")
        real_heartbeat()

    runner.tenure.heartbeat = flaky_heartbeat
    out = runner.run()
    assert not out.completed and "tenure lost" in (out.reason or "")
    assert store.completion() is None
    assert read_json(store.p("breaker.json")) is None
    assert read_json(store.p("runs", f"{out.run_id}.json")) is None


@test("§10: breaker reset is rejected while a live run owns tenure")
def _():
    store = tmpstore()
    owner = Tenure(store, heartbeat_period=60)
    owner.acquire("owner")
    try:
        Runner(store, [Step("s", invoke=lambda _ctx: ok())]).reset_breaker()
        raise AssertionError("breaker reset raced a live run")
    except TenureUnavailable:
        pass
    finally:
        owner.release()


@test("G4: a non-boolean postcondition is ambiguity and therefore failure")
def _():
    store = tmpstore()
    out = Runner(store, [Step(
        "s", invoke=lambda _ctx: ok(),
        postcondition=lambda _ctx, _result: "yes",
    )]).run()
    assert not out.completed
    assert "non-boolean" in (out.reason or "")


@test("G4/G3: malformed result metadata fails with an owned run record")
def _():
    bad_results = [
        {"ok": "yes"},
        {"ok": True, "positions": []},
        {"ok": True, "degraded": "source-a"},
        {"ok": True, "positions": {"source-a": float("nan")}},
        {"ok": True, "paused": True},
    ]
    for bad in bad_results:
        store = tmpstore()
        out = Runner(store, [Step("s", invoke=lambda _ctx, value=bad: value)]).run()
        assert not out.completed, bad
        assert "invalid step result" in (out.reason or "")
        assert read_json(store.p("runs", f"{out.run_id}.json")) is not None


@test("G4/G5: a reconciled effect must still pass the normal postcondition")
def _():
    store = tmpstore()
    calls = []
    key = lambda _ctx: "publish:1"  # noqa: E731
    Runner(store, [Step(
        "publish",
        invoke=lambda _ctx: (_ for _ in ()).throw(RuntimeError("after effect")),
        side_effecting=True,
        idempotency_key=key,
    )]).run()
    out = Runner(store, [Step(
        "publish",
        invoke=lambda _ctx: (calls.append("invoked"), ok())[1],
        side_effecting=True,
        idempotency_key=key,
        reconcile=lambda _ctx, _intent: Reconciliation(EffectState.PRESENT),
        postcondition=lambda _ctx, _result: False,
    )]).run()
    assert not out.completed and calls == []
    assert out.steps["publish"] == "reconciled-postcondition-failed"
    assert read_json(store.p("intents", "publish.json")) is not None


@test("G5: a key callback must return a non-empty string before invocation")
def _():
    store = tmpstore()
    calls = []
    out = Runner(store, [Step(
        "publish", invoke=lambda _ctx: (calls.append("invoked"), ok())[1],
        side_effecting=True, idempotency_key=lambda _ctx: "",
    )]).run()
    assert not out.completed and calls == []
    assert "empty idempotency key" in (out.reason or "")
    assert read_json(store.p("intents", "publish.json")) is None


@test("G5: an unresolved intent blocks a changed idempotency key")
def _():
    store = tmpstore()
    Runner(store, [Step(
        "publish", invoke=lambda _ctx: (_ for _ in ()).throw(RuntimeError("crash")),
        side_effecting=True, idempotency_key=lambda _ctx: "old-key",
    )]).run()
    calls = []
    out = Runner(store, [Step(
        "publish", invoke=lambda _ctx: (calls.append("invoke"), ok())[1],
        side_effecting=True, idempotency_key=lambda _ctx: "new-key",
        reconcile=lambda _ctx, _intent: (calls.append("reconcile"),
                                           Reconciliation(EffectState.ABSENT))[1],
    )]).run()
    assert not out.completed and calls == []
    assert out.steps["publish"] == "idempotency-key-mismatch"
    assert read_json(store.p("intents", "publish.json"))["idempotency_key"] == "old-key"


@test("G5: unknown reconciliation stops instead of being treated as absence")
def _():
    store = tmpstore()
    key = lambda _ctx: "stable-key"  # noqa: E731
    Runner(store, [Step(
        "publish", invoke=lambda _ctx: (_ for _ in ()).throw(RuntimeError("crash")),
        side_effecting=True, idempotency_key=key,
    )]).run()
    calls = []
    out = Runner(store, [Step(
        "publish", invoke=lambda _ctx: (calls.append("invoke"), ok())[1],
        side_effecting=True, idempotency_key=key,
        reconcile=lambda _ctx, _intent: Reconciliation(EffectState.UNKNOWN),
    )]).run()
    assert not out.completed and calls == []
    assert out.steps["publish"] == "reconciliation-unknown"
    assert read_json(store.p("intents", "publish.json")) is not None


@test("G5: confirmed absence retries through the original key")
def _():
    store = tmpstore()
    key = lambda _ctx: "stable-key"  # noqa: E731
    Runner(store, [Step(
        "publish", invoke=lambda _ctx: (_ for _ in ()).throw(RuntimeError("before effect")),
        side_effecting=True, idempotency_key=key,
    )]).run()
    observed = []

    def retry(ctx):
        observed.append(ctx.idempotency_key)
        return ok()

    out = Runner(store, [Step(
        "publish", invoke=retry, side_effecting=True, idempotency_key=key,
        reconcile=lambda _ctx, _intent: Reconciliation(EffectState.ABSENT),
    )]).run()
    assert out.completed and observed == ["stable-key"]
    assert read_json(store.p("intents", "publish.json")) is None


@test("G5: a side-effecting pause is a failure and retains its intent")
def _():
    store = tmpstore()
    out = Runner(store, [Step(
        "publish", invoke=lambda _ctx: {"ok": True, "paused": "approve"},
        side_effecting=True, idempotency_key=lambda _ctx: "publish:pause",
    )]).run()
    assert not out.completed and out.steps["publish"] == "unsafe-pause"
    assert read_json(store.p("intents", "publish.json")) is not None
    assert store.completion() is None


@test("G5: a verified effect keeps its intent when a later step fails")
def _():
    store = tmpstore()
    external = store.p("external.txt")
    key = lambda _ctx: "publish:one"  # noqa: E731

    first = Runner(store, [
        Step(
            "publish",
            invoke=lambda _ctx: (atomic_write(external, "once"), ok())[1],
            side_effecting=True,
            idempotency_key=key,
            postcondition=lambda _ctx, _result: os.path.exists(external),
        ),
        Step("later", invoke=lambda _ctx: {"ok": False}, depends_on=["publish"]),
    ]).run()
    assert not first.completed
    assert read_json(store.p("intents", "publish.json")) is not None

    calls = []
    second = Runner(store, [
        Step(
            "publish",
            invoke=lambda _ctx: (calls.append("duplicate"), ok())[1],
            side_effecting=True,
            idempotency_key=key,
            reconcile=lambda _ctx, _intent: Reconciliation(EffectState.PRESENT),
            postcondition=lambda _ctx, _result: os.path.exists(external),
        ),
        Step("later", invoke=lambda _ctx: ok(), depends_on=["publish"]),
    ]).run()
    assert second.completed and calls == []
    assert open(external).read() == "once"


@test("G5: an intent left after durable completion is cleanup residue")
def _():
    store = tmpstore()
    first_runner = Runner(store, [Step(
        "publish", invoke=lambda _ctx: ok(), side_effecting=True,
        idempotency_key=lambda _ctx: "publish:old",
    )])
    first_runner._clear_intent = lambda _ctx, _step: None
    first = first_runner.run()
    intent = read_json(store.p("intents", "publish.json"))
    assert first.completed and intent["run_id"] == first.run_id

    calls = []
    second = Runner(store, [Step(
        "publish", invoke=lambda _ctx: (calls.append("new-effect"), ok())[1],
        side_effecting=True, idempotency_key=lambda _ctx: "publish:new",
    )]).run()
    assert second.completed and calls == ["new-effect"]
    assert read_json(store.p("intents", "publish.json")) is None


@test("G6: structurally invalid lock JSON is a seizable corrupt lock")
def _():
    store = tmpstore()
    atomic_write(store.p("lock.json"), '{"run_id":"missing-fields"}')
    tenure = Tenure(store, heartbeat_period=60)
    rec = tenure.acquire("successor")
    assert rec.run_id == "successor"
    assert any("corrupt" in item for item in tenure.breaks)
    tenure.release()


@test("configuration: invalid lease and budget values are rejected early")
def _():
    store = tmpstore()
    bad_factories = [
        lambda: Tenure(store, heartbeat_period=0),
        lambda: Tenure(store, ttl_multiple=0),
        lambda: Tenure(store, clock_skew_allowance=-1),
        lambda: Runner(store, [], budget=Budget(max_steps=-1)),
        lambda: Runner(store, [], budget=Budget(max_seconds=0)),
        lambda: Runner(store, [], budget=Budget(max_consecutive_failures=0)),
    ]
    for build in bad_factories:
        try:
            build()
            raise AssertionError("invalid configuration was accepted")
        except RatchetError:
            pass


if __name__ == "__main__":
    sys.exit(main())
