"""
Ratchet Runtime — reference implementation of the execution-safety contract.

Implements docs/CONTRACT.md v0.5.0-alpha.2:
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
import math
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

__version__ = "0.6.0a1"


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


class EffectState(str, Enum):
    """What a target-system read-back established about an orphaned effect."""

    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Reconciliation:
    """Tri-state recovery decision plus evidence for the normal postcondition."""

    state: EffectState
    result: Dict[str, Any] = field(default_factory=dict)


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
        completion = read_json(self.p("completion.json"))
        if completion is None:
            return None
        if (not isinstance(completion, dict)
                or not isinstance(completion.get("completed_at"), str)
                or not isinstance(completion.get("run_id"), str)
                or not isinstance(completion.get("positions"), dict)
                or not isinstance(completion.get("outcome"), dict)):
            raise RatchetError("corrupt completion record: invalid schema")
        return completion

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
    tenure: int
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
        if heartbeat_period <= 0:
            raise RatchetError("heartbeat_period must be greater than zero")
        if ttl_multiple <= 0:
            raise RatchetError("ttl_multiple must be greater than zero")
        if clock_skew_allowance < 0:
            raise RatchetError("clock_skew_allowance must not be negative")
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
        cur = read_json(self.counter_path)
        if cur is None:
            current = 0
        elif (not isinstance(cur, dict) or type(cur.get("tenure")) is not int
              or cur["tenure"] < 0):
            raise RatchetError("corrupt fencing counter")
        else:
            current = cur["tenure"]
        nxt = current + 1
        atomic_write(self.counter_path, json.dumps({"tenure": nxt}))
        return nxt

    def _write(self, rec: LockRecord) -> None:
        atomic_write(self.path, json.dumps(rec.__dict__, indent=2, sort_keys=True))

    def _read(self) -> Optional[LockRecord]:
        d = read_json(self.path)
        if d is None:
            return None
        try:
            rec = LockRecord(**d)
        except (TypeError, ValueError) as e:
            raise RatchetError(f"corrupt lock record: {e}") from e
        valid = (
            isinstance(rec.run_id, str) and bool(rec.run_id)
            and type(rec.pid) is int
            and isinstance(rec.host, str) and bool(rec.host)
            and type(rec.tenure) is int and rec.tenure > 0
            and isinstance(rec.started_at, (int, float)) and not isinstance(rec.started_at, bool)
            and math.isfinite(rec.started_at)
            and isinstance(rec.heartbeat_at, (int, float)) and not isinstance(rec.heartbeat_at, bool)
            and math.isfinite(rec.heartbeat_at)
        )
        if not valid:
            raise RatchetError("corrupt lock record: invalid field type or value")
        return rec

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
                except Exception as e:                              # fail closed on I/O/state errors
                    self._heartbeat_error = TenureLost(f"heartbeat failed: {e!r}")
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

_STEP_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


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

    reconcile: Optional[Callable[["RunContext", dict], Reconciliation]] = None
    """G5. Classifies a recovered effect as present, absent, or unknown."""

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _STEP_NAME.fullmatch(self.name):
            raise RatchetError(
                "step name must be 1-128 characters using letters, digits, '.', '_' or '-'"
            )
        if not callable(self.invoke):
            raise RatchetError(f"step {self.name} requires a callable invoke")
        if not callable(self.postcondition):
            raise RatchetError(f"step {self.name} requires a runner-evaluated postcondition")
        if (not isinstance(self.depends_on, list)
                or any(not isinstance(d, str) or not _STEP_NAME.fullmatch(d)
                       for d in self.depends_on)):
            raise RatchetError(f"step {self.name} has an invalid dependency declaration")
        if self.side_effecting and not callable(self.idempotency_key):
            raise RatchetError(f"side-effecting step {self.name} requires an idempotency key")
        if self.at_most_once and not self.side_effecting:
            raise RatchetError(f"at_most_once step {self.name} must be side-effecting")
        if self.reconcile is not None and not self.side_effecting:
            raise RatchetError(f"reconcile on step {self.name} requires side_effecting=True")


@dataclass
class RunContext:
    run_id: str
    positions: Dict[str, Any]
    tenure_token: int
    scratch: Dict[str, Any] = field(default_factory=dict)
    step_name: Optional[str] = None
    idempotency_key: Optional[str] = None


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
        if self.budget.max_steps < 0:
            raise RatchetError("max_steps must not be negative")
        if self.budget.max_seconds <= 0:
            raise RatchetError("max_seconds must be greater than zero")
        if self.budget.max_consecutive_failures < 1:
            raise RatchetError("max_consecutive_failures must be at least one")
        self.tenure = Tenure(store, heartbeat_period=heartbeat_period)
        _assert_dag(steps)

    # -- circuit breaker (§10) ------------------------------------------------
    def _breaker_path(self) -> str:
        return self.store.p("breaker.json")

    def _breaker(self) -> dict:
        b = read_json(self._breaker_path())
        if b is None:
            return {"consecutive_failures": 0, "open": False}
        if (not isinstance(b, dict)
                or type(b.get("consecutive_failures")) is not int
                or b["consecutive_failures"] < 0
                or type(b.get("open")) is not bool):
            raise RatchetError("corrupt circuit-breaker state")
        return b

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
        with self.tenure._guard():
            cur = self.tenure._read()
            if cur is not None:
                age = time.time() - cur.heartbeat_at
                if age <= self.tenure.ttl + self.tenure.clock_skew_allowance:
                    raise TenureUnavailable("cannot reset the breaker while a live run holds tenure")
            atomic_write(
                self._breaker_path(),
                json.dumps({"consecutive_failures": 0, "open": False}),
            )

    # -- intent records (G5) --------------------------------------------------
    def _intent_path(self, step: str) -> str:
        return self.store.p("intents", f"{step}.json")

    def _write_intent(self, ctx: RunContext, step: Step, key: str) -> None:
        atomic_write(self._intent_path(step.name),
                     json.dumps({"run_id": ctx.run_id, "step": step.name,
                                 "idempotency_key": key, "tenure": ctx.tenure_token,
                                 "at": _utc_now_iso()}))

    def _clear_intent(self, ctx: RunContext, step: Step) -> None:
        p = self._intent_path(step.name)
        if os.path.exists(p):
            os.unlink(p)
            dfd = os.open(os.path.dirname(p), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)

    def _orphan_intent(self, step: Step) -> Optional[dict]:
        intent = read_json(self._intent_path(step.name))
        if intent is None:
            return None
        if (not isinstance(intent, dict)
                or not isinstance(intent.get("run_id"), str)
                or intent.get("step") != step.name
                or not isinstance(intent.get("idempotency_key"), str)
                or not intent["idempotency_key"].strip()
                or type(intent.get("tenure")) is not int):
            raise RatchetError(f"corrupt intent record for step {step.name}")
        return intent

    def _effect_key(self, ctx: RunContext, step: Step) -> str:
        try:
            key = step.idempotency_key(ctx) if step.idempotency_key else None
        except Exception as e:
            raise RatchetError(f"idempotency key for step {step.name} raised: {e!r}") from e
        if not isinstance(key, str) or not key.strip():
            raise RatchetError(f"side-effecting step {step.name} produced an empty idempotency key")
        return key

    def _postcondition(self, ctx: RunContext, step: Step, result: dict) -> tuple[bool, str]:
        try:
            held = step.postcondition(ctx, result)
        except Exception as e:
            return False, f"postcondition raised: {e!r}"
        if type(held) is not bool:
            return False, f"postcondition returned non-boolean value: {held!r}"
        return held, "post-condition failed despite ok=true"

    @staticmethod
    def _result_error(result: Any) -> Optional[str]:
        if not isinstance(result, dict) or type(result.get("ok")) is not bool:
            return "result must be a dict containing boolean 'ok'"
        positions = result.get("positions", {})
        if positions is None:
            positions = {}
        if (not isinstance(positions, dict)
                or any(not isinstance(src, str) or not src for src in positions)):
            return "positions must be a mapping with non-empty string source names"
        degraded = result.get("degraded", [])
        if degraded is None:
            degraded = []
        if (not isinstance(degraded, list)
                or any(not isinstance(src, str) or not src for src in degraded)):
            return "degraded must be a list of non-empty source names"
        if "paused" in result and (
                not isinstance(result["paused"], str) or not result["paused"].strip()):
            return "paused must be a non-empty reason string when present"
        try:
            json.dumps({"positions": positions, "degraded": degraded}, allow_nan=False)
        except (TypeError, ValueError) as e:
            return f"result metadata must be finite JSON data: {e}"
        return None

    @staticmethod
    def _apply_metadata(result: dict, pending_positions: Dict[str, Any],
                        outcome: RunOutcome) -> None:
        for src, pos in (result.get("positions") or {}).items():
            pending_positions[src] = pos
        outcome.degraded.extend(result.get("degraded") or [])

    def _fail(self, outcome: RunOutcome, step: Step, reason: str,
              state: Optional[str] = None) -> RunOutcome:
        outcome.failed_step = step.name
        outcome.reason = reason
        if state:
            outcome.steps[step.name] = state
        return self._finish(outcome, ok=False)

    # -- run ------------------------------------------------------------------
    def run(self) -> RunOutcome:
        b = self._breaker()
        if b.get("open"):
            raise CircuitOpen(
                f"{b['consecutive_failures']} consecutive failures; explicit reset required")

        run_id = uuid.uuid4().hex[:12]
        outcome = RunOutcome(run_id=run_id, completed=False)
        started = time.monotonic()

        acquired = self.tenure.acquire(
            run_id, on_break=lambda rec: outcome.tenure_breaks.append(rec.run_id))
        try:
            # Everything after acquisition belongs inside the cleanup boundary. A corrupt
            # completion file must not strand a live heartbeat and make the lock immortal.
            self.tenure.start_heartbeat()
            last_completion = self.store.completion()
            ctx = RunContext(
                run_id=run_id,
                positions=dict(last_completion["positions"]) if last_completion else {},
                tenure_token=acquired.tenure,
            )
            pending_positions = dict(ctx.positions)
            resolved_intents: List[Step] = []
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
                    return self._fail(outcome, step, f"unmet dependencies: {missing}")

                self.tenure.heartbeat()
                ctx.step_name = step.name
                ctx.idempotency_key = None

                effect_key: Optional[str] = None
                if step.side_effecting:
                    try:
                        effect_key = self._effect_key(ctx, step)
                    except RatchetError as e:
                        return self._fail(outcome, step, str(e), "invalid-idempotency-key")
                    ctx.idempotency_key = effect_key

                # G5 — an orphan intent means a previous run may have landed the effect.
                orphan = self._orphan_intent(step)
                if (orphan and last_completion
                        and orphan["run_id"] == last_completion["run_id"]):
                    # Completion is authoritative. This intent survived only because cleanup
                    # failed or the process died after the commit; it is safe to remove before
                    # evaluating the next logical effect key.
                    self.tenure.mutate(lambda: self._clear_intent(ctx, step))
                    orphan = None
                if orphan and orphan.get("run_id") != run_id:
                    if not step.side_effecting or effect_key is None:
                        return self._fail(
                            outcome, step, "orphan intent belongs to a non-side-effecting step")
                    if orphan["idempotency_key"] != effect_key:
                        return self._fail(
                            outcome,
                            step,
                            "idempotency key changed while an orphan intent is unresolved",
                            "idempotency-key-mismatch",
                        )
                    if step.reconcile is None:
                        if step.at_most_once:
                            return self._fail(
                                outcome, step,
                                "at_most_once step has an unresolved orphan intent")
                        return self._fail(
                            outcome, step, "orphan intent requires reconciliation before retry")
                    try:
                        recovery = step.reconcile(ctx, orphan)
                    except Exception as e:
                        return self._fail(
                            outcome, step, f"reconciliation raised: {e!r}",
                            "reconciliation-error")

                    self.tenure.assert_held()
                    if time.monotonic() - started > self.budget.max_seconds:
                        raise BudgetExceeded(f"max_seconds={self.budget.max_seconds}")
                    if (not isinstance(recovery, Reconciliation)
                            or not isinstance(recovery.state, EffectState)
                            or not isinstance(recovery.result, dict)):
                        return self._fail(
                            outcome, step,
                            "reconciliation must return Reconciliation with a tri-state effect",
                            "reconciliation-invalid")
                    if recovery.state is EffectState.UNKNOWN:
                        return self._fail(
                            outcome, step, "external effect remains ambiguous; human review required",
                            "reconciliation-unknown")
                    if recovery.state is EffectState.PRESENT:
                        recovered_result = dict(recovery.result)
                        recovered_result["ok"] = True
                        recovered_result["reconciled"] = True
                        error = self._result_error(recovered_result)
                        if error or "paused" in recovered_result:
                            return self._fail(
                                outcome, step,
                                f"invalid reconciled result: {error or 'pause is not recoverable evidence'}",
                                "reconciliation-invalid")
                        held, reason = self._postcondition(ctx, step, recovered_result)
                        if not held:
                            return self._fail(
                                outcome, step, reason, "reconciled-postcondition-failed")
                        if time.monotonic() - started > self.budget.max_seconds:
                            raise BudgetExceeded(f"max_seconds={self.budget.max_seconds}")
                        self._apply_metadata(recovered_result, pending_positions, outcome)
                        outcome.reconciled.append(step.name)
                        outcome.steps[step.name] = "reconciled"
                        done.add(step.name)
                        resolved_intents.append(step)
                        continue
                    if step.at_most_once:
                        return self._fail(
                            outcome, step,
                            "at_most_once step has an unresolved orphan intent")
                    # ABSENT is the only state that reaches retry, and the key comparison
                    # above proves the retry uses the same logical operation identity.

                if step.side_effecting:
                    assert effect_key is not None
                    self.tenure.mutate(lambda: self._write_intent(ctx, step, effect_key))

                # Invoke. The result is INPUT to the decision, never the decision (§9).
                try:
                    result = step.invoke(ctx)
                except Exception as e:
                    return self._fail(outcome, step, f"step raised: {e!r}")

                # A background heartbeat may have discovered seizure while the step ran.
                # Revalidate before accepting any result or changing runner state.
                self.tenure.assert_held()
                if time.monotonic() - started > self.budget.max_seconds:
                    raise BudgetExceeded(f"max_seconds={self.budget.max_seconds}")

                error = self._result_error(result)
                if error:
                    return self._fail(
                        outcome, step, f"invalid step result: {error}", "invalid-result")

                if result["ok"] is not True:
                    return self._fail(
                        outcome, step,
                        f"step reported failure: {result.get('diagnostics')}")

                # G4 — the agent's report is necessary, never sufficient.
                held, reason = self._postcondition(ctx, step, result)
                if not held:
                    return self._fail(outcome, step, reason, "postcondition-failed")
                if time.monotonic() - started > self.budget.max_seconds:
                    raise BudgetExceeded(f"max_seconds={self.budget.max_seconds}")

                # A workflow may deliberately stop for human approval. Pending positions stay
                # uncommitted, no completion marker is written, and the circuit breaker is
                # unchanged. The step's post-condition still ran above, so `paused` cannot be
                # used to bypass verification of the pause artifact itself.
                if "paused" in result:
                    if step.side_effecting:
                        return self._fail(
                            outcome,
                            step,
                            "a side-effecting step cannot pause after invocation; intent retained",
                            "unsafe-pause",
                        )
                    outcome.steps[step.name] = "paused"
                    outcome.paused_at = step.name
                    outcome.reason = result["paused"]
                    return self._finish(outcome, ok=None)

                # G3 — positions are held PENDING; committed only at verified completion.
                self._apply_metadata(result, pending_positions, outcome)

                if step.side_effecting:
                    # Keep the intent until the WHOLE workflow commits. Clearing it here would
                    # lose recovery evidence if a later step failed or the process died before
                    # completion.json became durable.
                    resolved_intents.append(step)
                outcome.steps[step.name] = "ok"
                done.add(step.name)

            # G4 + G6 — marker, positions and outcome in ONE atomic durable commit.
            outcome.completed = True

            def commit_success() -> None:
                self.store.commit_completion(run_id, pending_positions, _outcome_dict(outcome))
                self._record_run(True)
                self._write_run_record(outcome)
                for resolved in resolved_intents:
                    try:
                        self._clear_intent(ctx, resolved)
                    except OSError:
                        # The durable completion record makes this cleanup residue recognizable
                        # and safe to remove at the start of the next run.
                        pass

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
