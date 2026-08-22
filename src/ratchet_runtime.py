"""
Ratchet Runtime — reference implementation of the execution-safety contract.

Implements docs/CONTRACT.md v0.5.0-alpha.1:
  G1 Ordering · G2 Exclusive write tenure · G3 Position integrity (conditional)
  G4 Verified completion · G5 Effect integrity · G6 State isolation and durability
  plus §10 Boundedness.

Design rule: this module contains NO domain semantics. It never inspects what a step
produced beyond the post-condition the step itself declared. If a change here requires
understanding what the workflow is about, it belongs in a step.

No third-party dependencies. The tenure guard uses POSIX ``flock`` and therefore
targets local filesystems on Linux and macOS; network filesystems are out of scope.
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

__version__ = "0.5.0a1"


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────

class RatchetError(Exception): ...
class TenureLost(RatchetError):
    """Held tenure was taken by another run. The run MUST NOT write again (G2)."""
class TenureUnavailable(RatchetError):
    """Another live run holds tenure."""
class BudgetExceeded(RatchetError):
    """§10 — a declared cap was hit."""
class CircuitOpen(RatchetError):
    """§10 — too many consecutive failures; requires explicit human reset."""


# ─────────────────────────────────────────────────────────────────────────────
# G6 · Atomic, durable state
# ─────────────────────────────────────────────────────────────────────────────

def atomic_write(path: str, data: str) -> None:
    """Temp → fsync → rename → fsync(dir). A crash leaves old or new, never a mixture."""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, f".{os.path.basename(path)}.{uuid.uuid4().hex}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_json(path: str) -> Optional[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        # A torn file is not silently treated as absent — absence and corruption differ.
        raise RatchetError(f"corrupt state file: {path}")


class StateStore:
    """
    Runner-owned state. §8 requires this be unwritable by agent steps; enforced here by
    0o700 on the directory. A deployment where a step runs as the same user must isolate
    by process user or a mediated API instead — the mode bits are necessary, not sufficient.
    """

    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, mode=0o700, exist_ok=True)
        os.chmod(self.root, 0o700)

    def p(self, *parts: str) -> str:
        return os.path.join(self.root, *parts)

    # -- commit: marker + positions + outcome, ONE atomic write (§8) --------
    def commit_completion(self, run_id: str, positions: Dict[str, Any], outcome: dict) -> None:
        payload = {
            "completed_at": _utc_now_iso(),
            "run_id": run_id,
            "positions": positions,
            "outcome": outcome,
        }
        atomic_write(self.p("completion.json"), json.dumps(payload, indent=2, sort_keys=True))

    def completion(self) -> Optional[dict]:
        return read_json(self.p("completion.json"))

    def positions(self) -> Dict[str, Any]:
        c = self.completion()
        return dict(c["positions"]) if c else {}


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ─────────────────────────────────────────────────────────────────────────────
# G2 · Exclusive write tenure
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class LockRecord:
    run_id: str
    pid: int
    host: str
    tenure: Optional[int]
    started_at: float      # epoch seconds, UTC — see the note below
    heartbeat_at: float


# §12 amendment (v0.3): a PERSISTED heartbeat cannot be monotonic.
#
# v0.2's §12 said "lock TTL and heartbeat use a monotonic clock, never wall time", and the
# first implementation obeyed it. That rule is correct for measuring a duration inside one
# process and wrong for a lease, which is by definition read by a DIFFERENT process than
# wrote it. time.monotonic()'s reference point is explicitly undefined across processes and
# is meaningless across hosts — and LockRecord carries a `host` field, so cross-host was
# always in scope. A monotonic value from another clock domain yields a garbage age, which
# either breaks a live lock instantly or never breaks a dead one.
#
# So: wall clock, plus an explicit skew allowance. That is not a weakening. A lease can
# never be made safe by clock precision alone — the tenure counter (a fencing token) is
# what makes it safe, and clock error only costs availability, never correctness.
CLOCK_SKEW_ALLOWANCE = 30.0  # seconds; added to TTL before declaring a lock stale


class Tenure:
    """
    A lease, honestly named. TTL derives from the HEARTBEAT PERIOD, not run duration —
    v0.1's "2x median run duration" tracked the fast mode of a bimodal workload and so
    broke the busy-day run, and fed back on itself.
    """

    def __init__(self, store: StateStore, heartbeat_period: float = 1.0, ttl_multiple: float = 5.0,
                 clock_skew_allowance: float = CLOCK_SKEW_ALLOWANCE):
        self.store = store
        self.heartbeat_period = heartbeat_period
        self.ttl = heartbeat_period * ttl_multiple
        self.clock_skew_allowance = clock_skew_allowance
        self.path = store.p("lock.json")
        self.counter_path = store.p("tenure_counter.json")
        self.guard_path = store.p(".tenure.guard")
        self.held: Optional[LockRecord] = None
        self.breaks: List[str] = []
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_error: Optional[TenureLost] = None

    @contextmanager
    def _guard(self):
        """Serialize lock inspection, fencing-token allocation, and runner-state writes."""
        os.makedirs(self.store.root, mode=0o700, exist_ok=True)
        fd = os.open(self.guard_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _next_tenure_locked(self) -> int:
        """Allocate a unique fencing token while ``_guard`` is held."""
        cur = read_json(self.counter_path) or {"tenure": 0}
        nxt = int(cur["tenure"]) + 1
        atomic_write(self.counter_path, json.dumps({"tenure": nxt}))
        return nxt

    def _write(self, rec: LockRecord) -> None:
        atomic_write(self.path, json.dumps(rec.__dict__, indent=2, sort_keys=True))

    def _read(self) -> Optional[LockRecord]:
        d = read_json(self.path)
        return LockRecord(**d) if d else None

    def acquire(self, run_id: str, on_break: Optional[Callable[[LockRecord], None]] = None) -> LockRecord:
        """Acquire or seize tenure as one serialized local-filesystem transaction."""
        with self._guard():
            corrupt: Optional[str] = None
            try:
                cur = self._read()
            except RatchetError as e:
                corrupt = str(e)
                cur = None

            if cur is not None:
                age = time.time() - cur.heartbeat_at
                if age <= self.ttl + self.clock_skew_allowance:
                    raise TenureUnavailable(
                        f"run {cur.run_id} holds tenure (heartbeat {age:.1f}s ago, "
                        f"ttl {self.ttl:.1f}s + {self.clock_skew_allowance:.0f}s skew allowance)")
                self.breaks.append(cur.run_id)
                if on_break:
                    on_break(cur)
            elif corrupt is not None:
                self.breaks.append(f"<corrupt:{corrupt}>")

            now = time.time()
            acquired = LockRecord(
                run_id=run_id, pid=os.getpid(), host=_host(),
                tenure=self._next_tenure_locked(),
                started_at=now, heartbeat_at=now,
            )
            self._write(acquired)
            self.held = acquired
            self._heartbeat_error = None
            return acquired

    def start_heartbeat(self) -> None:
        """Keep a Runner-owned tenure fresh while a step is executing."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()

        def beat() -> None:
            while not self._heartbeat_stop.wait(self.heartbeat_period):
                try:
                    self.heartbeat()
                except TenureLost as e:
                    self._heartbeat_error = e
                    return

        self._heartbeat_thread = threading.Thread(
            target=beat, name="ratchet-heartbeat", daemon=True)
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread is not threading.current_thread():
            self._heartbeat_thread.join(timeout=max(1.0, self.heartbeat_period * 2))
        self._heartbeat_thread = None

    def heartbeat(self) -> None:
        if not self.held:
            return
        with self._guard():
            self._assert_held_locked()
            self.held.heartbeat_at = time.time()
            self._write(self.held)

    def _assert_held_locked(self) -> None:
        if not self.held:
            raise TenureLost("no tenure held")
        cur = self._read()
        if cur is None or cur.run_id != self.held.run_id or cur.tenure != self.held.tenure:
            raise TenureLost(
                f"tenure lost: held {self.held.run_id}/{self.held.tenure}, "
                f"found {getattr(cur, 'run_id', None)}/{getattr(cur, 'tenure', None)}")

    def assert_held(self) -> None:
        """
        §4's load-bearing clause. Called immediately before every state-mutating write.
        A run that has lost tenure aborts; it does not write.
        """
        if self._heartbeat_error:
            raise self._heartbeat_error
        with self._guard():
            self._assert_held_locked()

    def mutate(self, operation: Callable[[], Any]) -> Any:
        """Validate ownership and mutate runner state while seizure is excluded."""
        if self._heartbeat_error:
            raise self._heartbeat_error
        with self._guard():
            self._assert_held_locked()
            return operation()

    def release(self) -> None:
        """
        Ownership-checked (§4) — a run may only release its own lock.

        Release must SUCCEED on any filesystem that let us acquire, or a run holds its lock
        until TTL for no reason and the next scheduled run is blocked by a run that finished
        cleanly. So where unlink is denied, fall back to writing an already-expired record:
        the lock still exists, but any acquirer sees it as stale and seizes it immediately.
        Equivalent in effect, and reachable with only the rename permission we know we have.
        """
        self.stop_heartbeat()
        with self._guard():
            try:
                cur = self._read()
            except RatchetError:
                cur = None
            if cur and self.held and cur.run_id == self.held.run_id and cur.tenure == self.held.tenure:
                try:
                    os.unlink(self.path)
                except OSError:
                    expired = LockRecord(
                        run_id=self.held.run_id, pid=self.held.pid, host=self.held.host,
                        tenure=self.held.tenure,
                        started_at=self.held.started_at, heartbeat_at=0.0,
                    )
                    try:
                        self._write(expired)
                    except OSError:
                        pass
        self.held = None


def _host() -> str:
    try:
        import socket
        return socket.gethostname()
    except Exception:
        return "unknown"


def _terminate_if_reachable(rec: LockRecord) -> bool:
    """
    REMOVED in 0.4.0. Always returns False; kept so the call site still reads as the decision
    point, and so this reasoning sits where the next person will look for it.

    §4 said "breaking a stale lock MUST be accompanied by terminating the holder", and this
    function used to do that with `os.kill(rec.pid, SIGTERM)`.

    **PIDs are recycled.** The record's pid identifies a process that, by definition, we
    believe is dead or hung — so by the time we read it, the operating system may well have
    handed that number to something entirely unrelated. Signalling it terminates an innocent
    process, silently, with no way for anyone to connect the death to this runner. That is a
    far worse failure than the one the kill was meant to prevent, and it is not theoretical:
    a stale lock left by a crashed run caused this code to SIGTERM the shell that invoked it.

    The guard against self-termination (`rec.pid == os.getpid()`) does not help. It catches
    exactly one recycled pid — our own — out of every pid on the host.

    Making it safe would mean proving the pid is still the same process: comparing process
    start time, or cgroup, or an OS handle. All platform-specific, all fallible, and none of
    it necessary — because **fencing already provides the guarantee the kill was reaching
    for.** A dispossessed run calls `assert_held()` before every state-mutating write, sees a
    tenure it no longer owns, and aborts. It cannot write. The kill only narrowed a window
    that was already closed, at the cost of an unbounded external hazard.

    So the honest form of §4 is: **breaking a stale lock fences the holder out; it does not
    kill it.** A hung holder that never wakes costs nothing. One that wakes discovers it has
    been dispossessed and stops.
    """
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Steps
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Step:
    name: str
    invoke: Callable[["RunContext"], dict]
    """Performs the work and returns a result dict. In production this starts an agent
    session and reads its result FILE; the callable indirection is what makes every
    guarantee testable without an agent (§14)."""

    postcondition: Optional[Callable[["RunContext", dict], bool]] = None
    """G4. Runner-evaluated, deterministic. `ok: true` with a failing post-condition is a
    FAILED step. Without this the completion marker is a transcription of the agent's own
    self-report — which is what made v0.1's G4 false."""

    depends_on: List[str] = field(default_factory=list)
    side_effecting: bool = False
    idempotency_key: Optional[Callable[["RunContext"], str]] = None
    at_most_once: bool = False
    """G5. A step that cannot be made idempotent is never automatically retried."""

    reconcile: Optional[Callable[["RunContext", dict], bool]] = None
    """G5. Given a recovered intent record, did the effect land? Used instead of blind re-run."""

    def __post_init__(self) -> None:
        if not self.name:
            raise RatchetError("step name must not be empty")
        if self.postcondition is None:
            raise RatchetError(f"step {self.name} requires a runner-evaluated postcondition")
        if self.side_effecting and self.idempotency_key is None:
            raise RatchetError(f"side-effecting step {self.name} requires an idempotency key")
        if self.at_most_once and not self.side_effecting:
            raise RatchetError(f"at_most_once step {self.name} must be side-effecting")
        if self.reconcile is not None and not self.side_effecting:
            raise RatchetError(f"reconcile on step {self.name} requires side_effecting=True")


@dataclass
class RunContext:
    run_id: str
    store: StateStore
    positions: Dict[str, Any]
    scratch: Dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# The Runner
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Budget:
    max_steps: int = 100
    max_seconds: float = 3600.0
    max_consecutive_failures: int = 3


@dataclass
class RunOutcome:
    run_id: str
    completed: bool
    failed_step: Optional[str] = None
    reason: Optional[str] = None
    steps: Dict[str, str] = field(default_factory=dict)
    degraded: List[str] = field(default_factory=list)
    tenure_breaks: List[str] = field(default_factory=list)
    reconciled: List[str] = field(default_factory=list)
    paused_at: Optional[str] = None


class Runner:
    def __init__(self, store: StateStore, steps: List[Step],
                 budget: Optional[Budget] = None, heartbeat_period: float = 1.0):
        self.store = store
        self.steps = steps
        self.budget = budget or Budget()
        self.tenure = Tenure(store, heartbeat_period=heartbeat_period)
        _assert_dag(steps)

    # -- circuit breaker (§10) ------------------------------------------------
    def _breaker_path(self) -> str:
        return self.store.p("breaker.json")

    def _breaker(self) -> dict:
        return read_json(self._breaker_path()) or {"consecutive_failures": 0, "open": False}

    def _record_run(self, ok: Optional[bool]) -> None:
        # A deliberate human-approval pause is neither success nor failure. In particular it
        # must not clear a real failure streak or increment the breaker until routine previews
        # eventually brick the workflow.
        if ok is None:
            return
        b = self._breaker()
        if ok:
            b = {"consecutive_failures": 0, "open": False}
        else:
            b["consecutive_failures"] = int(b["consecutive_failures"]) + 1
            if b["consecutive_failures"] >= self.budget.max_consecutive_failures:
                b["open"] = True
        atomic_write(self._breaker_path(), json.dumps(b))

    def reset_breaker(self) -> None:
        """Explicit human reset — §10 requires the breaker not clear itself."""
        atomic_write(self._breaker_path(), json.dumps({"consecutive_failures": 0, "open": False}))

    # -- intent records (G5) --------------------------------------------------
    def _intent_path(self, run_id: str, step: str) -> str:
        return self.store.p("intents", f"{step}.json")

    def _write_intent(self, ctx: RunContext, step: Step) -> None:
        key = step.idempotency_key(ctx) if step.idempotency_key else None
        atomic_write(self._intent_path(ctx.run_id, step.name),
                     json.dumps({"run_id": ctx.run_id, "step": step.name,
                                 "idempotency_key": key, "at": _utc_now_iso()}))

    def _clear_intent(self, ctx: RunContext, step: Step) -> None:
        p = self._intent_path(ctx.run_id, step.name)
        if os.path.exists(p):
            os.unlink(p)

    def _orphan_intent(self, step: Step) -> Optional[dict]:
        return read_json(self._intent_path("", step.name))

    # -- run ------------------------------------------------------------------
    def run(self) -> RunOutcome:
        b = self._breaker()
        if b.get("open"):
            raise CircuitOpen(
                f"{b['consecutive_failures']} consecutive failures; explicit reset required")

        run_id = uuid.uuid4().hex[:12]
        outcome = RunOutcome(run_id=run_id, completed=False)
        started = time.monotonic()

        self.tenure.acquire(run_id, on_break=lambda rec: outcome.tenure_breaks.append(rec.run_id))
        self.tenure.start_heartbeat()
        ctx = RunContext(run_id=run_id, store=self.store, positions=self.store.positions())
        pending_positions = dict(ctx.positions)

        try:
            done: set[str] = set()
            for i, step in enumerate(self.steps):
                # §10 boundedness
                if i >= self.budget.max_steps:
                    raise BudgetExceeded(f"max_steps={self.budget.max_steps}")
                if time.monotonic() - started > self.budget.max_seconds:
                    raise BudgetExceeded(f"max_seconds={self.budget.max_seconds}")

                # G1 ordering
                missing = [d for d in step.depends_on if d not in done]
                if missing:
                    outcome.failed_step = step.name
                    outcome.reason = f"unmet dependencies: {missing}"
                    return self._finish(outcome, ok=False)

                self.tenure.heartbeat()

                # G5 — an orphan intent means a previous run may have landed the effect.
                orphan = self._orphan_intent(step)
                if orphan and orphan.get("run_id") != run_id:
                    if step.reconcile and step.reconcile(ctx, orphan):
                        outcome.reconciled.append(step.name)
                        outcome.steps[step.name] = "reconciled"
                        done.add(step.name)
                        self.tenure.mutate(lambda: self._clear_intent(ctx, step))
                        continue
                    if step.at_most_once:
                        outcome.failed_step = step.name
                        outcome.reason = "at_most_once step has an unreconciled orphan intent"
                        return self._finish(outcome, ok=False)
                    if step.reconcile is None:
                        outcome.failed_step = step.name
                        outcome.reason = "orphan intent requires reconciliation before retry"
                        return self._finish(outcome, ok=False)

                if step.side_effecting:
                    self.tenure.mutate(lambda: self._write_intent(ctx, step))

                # Invoke. The result is INPUT to the decision, never the decision (§9).
                try:
                    result = step.invoke(ctx)
                except Exception as e:
                    outcome.failed_step = step.name
                    outcome.reason = f"step raised: {e!r}"
                    return self._finish(outcome, ok=False)

                # A background heartbeat may have discovered seizure while the step ran.
                # Revalidate before accepting any result or changing runner state.
                self.tenure.assert_held()
                if time.monotonic() - started > self.budget.max_seconds:
                    raise BudgetExceeded(f"max_seconds={self.budget.max_seconds}")

                if not isinstance(result, dict) or "ok" not in result:
                    outcome.failed_step = step.name
                    outcome.reason = "invalid step result (ambiguity is failure)"
                    return self._finish(outcome, ok=False)

                if not result.get("ok"):
                    outcome.failed_step = step.name
                    outcome.reason = f"step reported failure: {result.get('diagnostics')}"
                    return self._finish(outcome, ok=False)

                # G4 — the agent's report is necessary, never sufficient.
                try:
                    held = bool(step.postcondition(ctx, result))
                except Exception as e:
                    held = False
                    result.setdefault("diagnostics", []).append(f"postcondition raised: {e!r}")
                if not held:
                    outcome.failed_step = step.name
                    outcome.reason = "post-condition failed despite ok=true"
                    outcome.steps[step.name] = "postcondition-failed"
                    return self._finish(outcome, ok=False)

                # A workflow may deliberately stop for human approval. Pending positions stay
                # uncommitted, no completion marker is written, and the circuit breaker is
                # unchanged. The step's post-condition still ran above, so `paused` cannot be
                # used to bypass verification of the pause artifact itself.
                if result.get("paused"):
                    self.tenure.mutate(lambda: self._clear_intent(ctx, step))
                    outcome.steps[step.name] = "paused"
                    outcome.paused_at = step.name
                    outcome.reason = str(result.get("paused"))
                    return self._finish(outcome, ok=None)

                # G3 — positions are held PENDING; committed only at verified completion.
                for src, pos in (result.get("positions") or {}).items():
                    pending_positions[src] = pos
                outcome.degraded.extend(result.get("degraded") or [])

                self.tenure.mutate(lambda: self._clear_intent(ctx, step))
                outcome.steps[step.name] = "ok"
                done.add(step.name)

            # G4 + G6 — marker, positions and outcome in ONE atomic durable commit.
            outcome.completed = True

            def commit_success() -> None:
                self.store.commit_completion(run_id, pending_positions, _outcome_dict(outcome))
                self._record_run(True)
                self._write_run_record(outcome)

            self.tenure.mutate(commit_success)
            return outcome

        except TenureLost as e:
            # This result stays in memory. Persisting it would itself violate G2.
            outcome.completed = False
            outcome.reason = f"tenure lost: {e}"
            return outcome
        except (BudgetExceeded, CircuitOpen) as e:
            outcome.reason = str(e)
            return self._finish(outcome, ok=False)
        finally:
            # Best-effort. A release that raises inside `finally` REPLACES the run's real
            # outcome with a cleanup error — a successful run reports as a crash, and a
            # failed one loses its diagnosis. Releasing is housekeeping; TTL covers the case
            # where it does not happen, and nothing about correctness depends on it.
            try:
                self.tenure.release()
            except Exception:                                       # noqa: BLE001
                pass

    def _finish(self, outcome: RunOutcome, ok: Optional[bool]) -> RunOutcome:
        def finish_owned() -> None:
            self._record_run(ok)
            self._write_run_record(outcome)

        self.tenure.mutate(finish_owned)
        return outcome

    def _write_run_record(self, outcome: RunOutcome) -> None:
        atomic_write(self.store.p("runs", f"{outcome.run_id}.json"),
                     json.dumps(_outcome_dict(outcome), indent=2, sort_keys=True))


def _outcome_dict(o: RunOutcome) -> dict:
    return {
        "run_id": o.run_id, "completed": o.completed, "failed_step": o.failed_step,
        "reason": o.reason, "steps": o.steps, "degraded": o.degraded,
        "tenure_breaks": o.tenure_breaks, "reconciled": o.reconciled,
        "paused_at": o.paused_at,
    }


def _assert_dag(steps: List[Step]) -> None:
    names = [s.name for s in steps]
    if len(set(names)) != len(names):
        raise RatchetError("duplicate step names")
    seen: set[str] = set()
    for s in steps:
        for d in s.depends_on:
            if d not in names:
                raise RatchetError(f"step {s.name} depends on unknown step {d}")
            if d not in seen:
                raise RatchetError(f"step {s.name} depends on {d}, which does not precede it")
        seen.add(s.name)


# ─────────────────────────────────────────────────────────────────────────────
# §11 · Watchdog — MUST run out of process
# ─────────────────────────────────────────────────────────────────────────────

def staleness_check(store: StateStore, max_age_seconds: float) -> Optional[str]:
    """
    Returns an alert string if the workflow has stopped completing.

    Deliberately a free function, not a Runner method: nothing inside a runner runs when
    the runner isn't running, so "nothing ran at all" — the failure that hides longest —
    can only be detected by a separate scheduled process.
    """
    c = store.completion()
    if c is None:
        return "no completion has ever been recorded"
    try:
        # calendar.timegm, NOT time.mktime — mktime interprets a struct_time as LOCAL
        # time, so it silently skews a UTC stamp by the host's offset. §12 exists because
        # of exactly this class of error, and the first run of this suite hit it.
        import calendar
        t = calendar.timegm(time.strptime(c["completed_at"], "%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        return f"unparseable completion timestamp: {c.get('completed_at')!r}"
    age = time.time() - t
    if age > max_age_seconds:
        return f"last completion {age:.0f}s ago, exceeds {max_age_seconds:.0f}s"
    return None
