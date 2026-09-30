"""Transactional SQLite candidate backend for Ratchet SQLite."""

from __future__ import annotations

import sqlite3
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from .contracts import (
    NON_WAIVABLE_REASON_CODES,
    CheckpointTier,
    CommitCertificate,
    CommitCohort,
    EffectReceipt,
    ProposedPosition,
    RunCommitBundle,
    TerminalOutcome,
    VerificationProof,
    VerificationStatus,
    canonical_digest,
    canonical_json,
    require_run_id,
)

from .comparators import Comparator, ComparatorRegistry
from .errors import (
    AlreadyCommitted,
    AttemptConflict,
    BundleRejected,
    CheckpointConflict,
    CohortIncomplete,
    EffectConflict,
    FenceRejected,
    InstanceNotFound,
    LeaseBusy,
    PositionRegression,
    ReadbackRequired,
    ReadOnlyRatchetError,
)
from .models import (
    ActiveSnapshot,
    AttemptHandle,
    AttemptSummary,
    CheckpointRecord,
    CommittedPosition,
    EffectIntent,
    EffectRecovery,
    InstanceStatus,
    Lease,
    RecoveryStatus,
)
from .tiers import checkpoint_is_enabled, effective_checkpoint_tier


_SCHEMA_VERSION = 1


def _require_text(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _rfc3339(epoch_seconds: float) -> str:
    return (
        datetime.fromtimestamp(epoch_seconds, timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _tier_from_name(name: str) -> CheckpointTier:
    try:
        return CheckpointTier[name]
    except KeyError as exc:
        raise RuntimeError(f"database contains unknown checkpoint tier {name!r}") from exc


def _outcome_from_name(name: str) -> TerminalOutcome:
    try:
        return TerminalOutcome[name]
    except KeyError as exc:
        raise RuntimeError(f"database contains unknown terminal outcome {name!r}") from exc


def _proof_is_verified(proof: VerificationProof) -> bool:
    return proof.status is VerificationStatus.VERIFIED


def _execute_atomic_script(connection: sqlite3.Connection, script: str) -> None:
    """Execute complete SQLite statements without ``executescript`` commits.

    ``Connection.executescript`` commits an open transaction before running its
    input. Schema creation therefore looked transactional while a mid-schema
    failure could actually leave a partially initialized Ratchet database.
    """

    pending: list[str] = []
    for line in script.splitlines():
        pending.append(line)
        statement = "\n".join(pending).strip()
        if not statement or not sqlite3.complete_statement(statement):
            continue
        connection.execute(statement)
        pending.clear()
    if "\n".join(pending).strip():
        raise RuntimeError("Ratchet schema contains an incomplete SQL statement")


class SQLiteRatchet:
    """A local transactional Ratchet implementation.

    A connection is opened per operation so separate processes and threads use
    SQLite's lock manager rather than sharing Python connection state. All
    mutating calls start with ``BEGIN IMMEDIATE`` and verify the current lease in
    that same transaction.
    """

    def __init__(
        self,
        database: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        id_factory: Callable[[], Any] = uuid.uuid4,
        comparators: ComparatorRegistry | None = None,
        busy_timeout_seconds: float = 5.0,
        read_only: bool = False,
    ) -> None:
        database_text = str(database)
        if database_text == ":memory:":
            raise ValueError("use a file-backed database; per-call connections cannot share :memory:")
        self.database = Path(database_text)
        self._read_only = bool(read_only)
        self._read_uri: str | None = None
        self._read_keeper: sqlite3.Connection | None = None
        if self._read_only:
            if not self.database.is_file():
                raise FileNotFoundError(f"read-only Ratchet database is missing: {self.database}")
        else:
            self.database.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._id_factory = id_factory
        self.comparators = comparators or ComparatorRegistry()
        self._busy_timeout_ms = max(1, int(busy_timeout_seconds * 1000))
        if self._read_only:
            self._initialize_read_only_snapshot()
            self._validate_schema_read_only()
        else:
            self._initialize_schema()

    @classmethod
    def open_read_only(
        cls,
        database: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        comparators: ComparatorRegistry | None = None,
        busy_timeout_seconds: float = 5.0,
    ) -> "SQLiteRatchet":
        """Open existing Ratchet state without schema, journal, or metadata writes."""

        return cls(
            database,
            clock=clock,
            comparators=comparators,
            busy_timeout_seconds=busy_timeout_seconds,
            read_only=True,
        )

    @property
    def read_only(self) -> bool:
        return self._read_only

    def register_comparator(self, position_type: str, comparator: Comparator) -> None:
        self.comparators.register(position_type, comparator)

    def _connect(self) -> sqlite3.Connection:
        if self._read_only:
            if self._read_uri is None or self._read_keeper is None:
                raise ReadOnlyRatchetError("read-only Ratchet snapshot is closed")
            connection = sqlite3.connect(
                self._read_uri,
                uri=True,
                timeout=self._busy_timeout_ms / 1000,
                isolation_level=None,
            )
        else:
            connection = sqlite3.connect(
                self.database,
                timeout=self._busy_timeout_ms / 1000,
                isolation_level=None,
            )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        if self._read_only:
            connection.execute("PRAGMA query_only = ON")
        else:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        return connection

    def _initialize_read_only_snapshot(self) -> None:
        wal = self.database.with_name(f"{self.database.name}-wal")
        shm = self.database.with_name(f"{self.database.name}-shm")
        wal_has_content = wal.exists() and bool(wal.stat().st_size)
        if wal_has_content and not shm.is_file():
            raise RuntimeError(
                "read-only Ratchet inspection cannot safely read an uncheckpointed "
                "WAL without its existing SHM sidecar"
            )
        self._read_uri = (
            f"file:ratchet-sqlite-read-only-{uuid.uuid4()}?mode=memory&cache=shared"
        )
        self._read_keeper = sqlite3.connect(
            self._read_uri, uri=True, isolation_level=None
        )
        try:
            if wal_has_content:
                source_paths = (self.database, wal, shm)
                before = tuple(
                    (path.stat().st_size, path.stat().st_mtime_ns, path.stat().st_ctime_ns)
                    for path in source_paths
                )
                payloads = {path.name: path.read_bytes() for path in source_paths}
                after = tuple(
                    (path.stat().st_size, path.stat().st_mtime_ns, path.stat().st_ctime_ns)
                    for path in source_paths
                )
                if after != before:
                    raise RuntimeError("Ratchet state changed during read-only snapshot capture")
                with tempfile.TemporaryDirectory(prefix="ratchet-sqlite-read-only-") as raw:
                    temporary = Path(raw)
                    for name, payload in payloads.items():
                        (temporary / name).write_bytes(payload)
                    source = sqlite3.connect(temporary / self.database.name)
                    try:
                        source.backup(self._read_keeper)
                    finally:
                        source.close()
            else:
                source = sqlite3.connect(
                    f"{self.database.resolve().as_uri()}?mode=ro&immutable=1",
                    uri=True,
                )
                try:
                    source.backup(self._read_keeper)
                finally:
                    source.close()
            self._read_keeper.execute("PRAGMA query_only = ON")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._read_keeper is not None:
            self._read_keeper.close()
            self._read_keeper = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    @contextmanager
    def _transaction(self, *, write: bool) -> Iterator[sqlite3.Connection]:
        if write and self._read_only:
            raise ReadOnlyRatchetError("Ratchet was opened read-only")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _validate_schema_read_only(self) -> None:
        with self._transaction(write=False) as connection:
            try:
                row = connection.execute(
                    "SELECT schema_version FROM ratchet_meta WHERE singleton = 1"
                ).fetchone()
            except sqlite3.OperationalError as error:
                raise RuntimeError("read-only Ratchet database has no supported schema") from error
            if row is None or row["schema_version"] != _SCHEMA_VERSION:
                observed = None if row is None else row["schema_version"]
                raise RuntimeError(
                    f"unsupported Ratchet database schema {observed}; expected {_SCHEMA_VERSION}"
                )

    def _initialize_schema(self) -> None:
        with self._transaction(write=True) as connection:
            _execute_atomic_script(
                connection,
                """
                CREATE TABLE IF NOT EXISTS ratchet_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS instances (
                    instance_id TEXT PRIMARY KEY,
                    next_fencing_token INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS leases (
                    instance_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    acquired_at TEXT NOT NULL,
                    acquired_epoch REAL NOT NULL,
                    expires_at TEXT NOT NULL,
                    expires_epoch REAL NOT NULL,
                    FOREIGN KEY (instance_id) REFERENCES instances(instance_id)
                );

                CREATE TABLE IF NOT EXISTS attempts (
                    instance_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    plan_digest TEXT NOT NULL,
                    effective_config_digest TEXT NOT NULL,
                    runtime_lock_digest TEXT NOT NULL,
                    requested_tier_name TEXT NOT NULL,
                    minimum_tier_name TEXT NOT NULL,
                    effective_tier_name TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('running', 'failed', 'committed')),
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    failure_reason TEXT,
                    PRIMARY KEY (instance_id, attempt_id),
                    FOREIGN KEY (instance_id) REFERENCES instances(instance_id)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS one_running_attempt_per_run
                ON attempts(instance_id, run_id) WHERE status = 'running';

                CREATE TABLE IF NOT EXISTS position_proposals (
                    instance_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    position_type TEXT NOT NULL,
                    encoded_value TEXT NOT NULL,
                    coverage_digest TEXT NOT NULL,
                    proposed_at TEXT NOT NULL,
                    PRIMARY KEY (instance_id, attempt_id, source_id),
                    FOREIGN KEY (instance_id, attempt_id)
                        REFERENCES attempts(instance_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS committed_positions (
                    instance_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    position_type TEXT NOT NULL,
                    encoded_value TEXT NOT NULL,
                    coverage_digest TEXT NOT NULL,
                    certificate_id TEXT NOT NULL,
                    committed_at TEXT NOT NULL,
                    PRIMARY KEY (instance_id, source_id),
                    FOREIGN KEY (instance_id) REFERENCES instances(instance_id)
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    instance_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    boundary_tier_name TEXT NOT NULL,
                    boundary_id TEXT NOT NULL,
                    proof_id TEXT NOT NULL,
                    proof_digest TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY (instance_id, attempt_id, boundary_tier_name, boundary_id),
                    FOREIGN KEY (instance_id, attempt_id)
                        REFERENCES attempts(instance_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS effects (
                    instance_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    effect_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    operation_digest TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN (
                            'prepared', 'dispatching', 'ambiguous', 'verified', 'cancelled'
                        )
                    ),
                    prepared_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    ambiguity_reason TEXT,
                    readback_digest TEXT,
                    verification_proof_id TEXT,
                    proof_digest TEXT,
                    receipt_json TEXT,
                    proof_json TEXT,
                    PRIMARY KEY (instance_id, effect_id),
                    UNIQUE (instance_id, idempotency_key),
                    FOREIGN KEY (instance_id, attempt_id)
                        REFERENCES attempts(instance_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS staged_bundles (
                    instance_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    bundle_digest TEXT NOT NULL,
                    bundle_json TEXT NOT NULL,
                    required_proof_types_json TEXT NOT NULL,
                    cohorts_json TEXT NOT NULL,
                    staged_at TEXT NOT NULL,
                    PRIMARY KEY (instance_id, attempt_id),
                    FOREIGN KEY (instance_id, attempt_id)
                        REFERENCES attempts(instance_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS certificates (
                    instance_id TEXT NOT NULL,
                    certificate_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    bundle_digest TEXT NOT NULL,
                    committed_at TEXT NOT NULL,
                    outcome_name TEXT NOT NULL,
                    active_snapshot_digest TEXT NOT NULL,
                    committed_position_digest TEXT NOT NULL,
                    previous_certificate_digest TEXT,
                    certificate_digest TEXT NOT NULL,
                    certificate_json TEXT NOT NULL,
                    PRIMARY KEY (instance_id, certificate_id),
                    UNIQUE (instance_id, attempt_id),
                    FOREIGN KEY (instance_id, attempt_id)
                        REFERENCES attempts(instance_id, attempt_id)
                );

                CREATE TABLE IF NOT EXISTS active_snapshots (
                    instance_id TEXT PRIMARY KEY,
                    snapshot_digest TEXT NOT NULL,
                    certificate_id TEXT NOT NULL,
                    activated_at TEXT NOT NULL,
                    FOREIGN KEY (instance_id, certificate_id)
                        REFERENCES certificates(instance_id, certificate_id)
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    instance_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    subject_id TEXT NOT NULL,
                    details_digest TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    FOREIGN KEY (instance_id) REFERENCES instances(instance_id)
                );
                """,
            )
            row = connection.execute(
                "SELECT schema_version FROM ratchet_meta WHERE singleton = 1"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO ratchet_meta(singleton, schema_version) VALUES (1, ?)",
                    (_SCHEMA_VERSION,),
                )
            elif row["schema_version"] != _SCHEMA_VERSION:
                raise RuntimeError(
                    f"unsupported Ratchet database schema {row['schema_version']}; "
                    f"expected {_SCHEMA_VERSION}"
                )

    def _audit(
        self,
        connection: sqlite3.Connection,
        instance_id: str,
        event_type: str,
        subject_id: str,
        details: object,
        occurred_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO audit_events(
                instance_id, event_type, subject_id, details_digest, occurred_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (instance_id, event_type, subject_id, canonical_digest(details), occurred_at),
        )

    def initialize_instance(self, instance_id: str) -> None:
        instance_id = _require_text("instance_id", instance_id)
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            connection.execute(
                """
                INSERT INTO instances(instance_id, created_at)
                VALUES (?, ?) ON CONFLICT(instance_id) DO NOTHING
                """,
                (instance_id, now),
            )

    def _require_instance(self, connection: sqlite3.Connection, instance_id: str) -> None:
        row = connection.execute(
            "SELECT 1 FROM instances WHERE instance_id = ?", (instance_id,)
        ).fetchone()
        if row is None:
            raise InstanceNotFound(instance_id)

    def acquire(self, instance_id: str, owner_id: str, *, ttl_seconds: float = 60.0) -> Lease:
        instance_id = _require_text("instance_id", instance_id)
        owner_id = _require_text("owner_id", owner_id)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        now_epoch = self._clock()
        now = _rfc3339(now_epoch)
        expires_epoch = now_epoch + ttl_seconds
        expires_at = _rfc3339(expires_epoch)
        with self._transaction(write=True) as connection:
            self._require_instance(connection, instance_id)
            current = connection.execute(
                "SELECT * FROM leases WHERE instance_id = ?", (instance_id,)
            ).fetchone()
            if current is not None and current["expires_epoch"] > now_epoch:
                raise LeaseBusy(
                    f"instance {instance_id!r} is held by {current['owner_id']!r} "
                    f"until {current['expires_at']}"
                )
            counter = connection.execute(
                "SELECT next_fencing_token FROM instances WHERE instance_id = ?",
                (instance_id,),
            ).fetchone()["next_fencing_token"]
            fencing_token = int(counter) + 1
            lease_id = f"lease-{self._id_factory()}"
            connection.execute(
                "UPDATE instances SET next_fencing_token = ? WHERE instance_id = ?",
                (fencing_token, instance_id),
            )
            connection.execute(
                """
                INSERT INTO leases(
                    instance_id, owner_id, lease_id, fencing_token,
                    acquired_at, acquired_epoch, expires_at, expires_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    lease_id = excluded.lease_id,
                    fencing_token = excluded.fencing_token,
                    acquired_at = excluded.acquired_at,
                    acquired_epoch = excluded.acquired_epoch,
                    expires_at = excluded.expires_at,
                    expires_epoch = excluded.expires_epoch
                """,
                (
                    instance_id,
                    owner_id,
                    lease_id,
                    fencing_token,
                    now,
                    now_epoch,
                    expires_at,
                    expires_epoch,
                ),
            )
            self._audit(
                connection,
                instance_id,
                "lease.acquired",
                lease_id,
                {"owner_id": owner_id, "fencing_token": fencing_token},
                now,
            )
        return Lease(
            instance_id=instance_id,
            owner_id=owner_id,
            lease_id=lease_id,
            fencing_token=fencing_token,
            acquired_at=now,
            expires_at=expires_at,
        )

    def _assert_position_advances(
        self,
        connection: sqlite3.Connection,
        instance_id: str,
        position: ProposedPosition,
    ) -> None:
        """Reject a position that does not advance the committed one.

        Called both when a position is proposed and again inside the commit
        transaction, because the committed state can move between those points.
        """
        committed = connection.execute(
            """
            SELECT * FROM committed_positions
            WHERE instance_id = ? AND source_id = ?
            """,
            (instance_id, position.source_id),
        ).fetchone()
        if committed is None:
            return
        if committed["position_type"] != position.position_type:
            raise PositionRegression(
                f"position type changed for source {position.source_id!r}"
            )
        try:
            comparison = self.comparators.compare(
                position.position_type,
                position.encoded_value,
                committed["encoded_value"],
            )
        except ValueError as exc:
            raise PositionRegression(
                f"invalid position for source {position.source_id!r}: {exc}"
            ) from exc
        if comparison < 0:
            raise PositionRegression(
                f"proposed position regresses source {position.source_id!r}"
            )

    def _assert_lease(self, connection: sqlite3.Connection, lease: Lease) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM leases WHERE instance_id = ?", (lease.instance_id,)
        ).fetchone()
        if (
            row is None
            or row["lease_id"] != lease.lease_id
            or row["owner_id"] != lease.owner_id
            or row["fencing_token"] != lease.fencing_token
        ):
            raise FenceRejected("lease no longer owns the instance")
        if row["expires_epoch"] <= self._clock():
            raise FenceRejected("lease has expired")
        return row

    def renew(self, lease: Lease, *, ttl_seconds: float = 60.0) -> Lease:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        now_epoch = self._clock()
        expires_epoch = now_epoch + ttl_seconds
        expires_at = _rfc3339(expires_epoch)
        now = _rfc3339(now_epoch)
        with self._transaction(write=True) as connection:
            current = self._assert_lease(connection, lease)
            connection.execute(
                """
                UPDATE leases SET expires_at = ?, expires_epoch = ?
                WHERE instance_id = ? AND lease_id = ? AND fencing_token = ?
                """,
                (
                    expires_at,
                    expires_epoch,
                    lease.instance_id,
                    lease.lease_id,
                    lease.fencing_token,
                ),
            )
            self._audit(
                connection,
                lease.instance_id,
                "lease.renewed",
                lease.lease_id,
                {"fencing_token": lease.fencing_token, "expires_at": expires_at},
                now,
            )
        return Lease(
            instance_id=lease.instance_id,
            owner_id=lease.owner_id,
            lease_id=lease.lease_id,
            fencing_token=lease.fencing_token,
            acquired_at=current["acquired_at"],
            expires_at=expires_at,
        )

    def release(self, lease: Lease) -> None:
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_lease(connection, lease)
            connection.execute(
                "DELETE FROM leases WHERE instance_id = ? AND lease_id = ?",
                (lease.instance_id, lease.lease_id),
            )
            self._audit(
                connection,
                lease.instance_id,
                "lease.released",
                lease.lease_id,
                {"fencing_token": lease.fencing_token},
                now,
            )

    def begin_attempt(
        self,
        lease: Lease,
        *,
        run_id: str,
        plan_digest: str,
        effective_config_digest: str,
        runtime_lock_digest: str,
        requested_tier: CheckpointTier,
        minimum_tier: CheckpointTier,
        attempt_id: str | None = None,
    ) -> AttemptHandle:
        run_id = require_run_id(run_id)
        plan_digest = _require_text("plan_digest", plan_digest)
        effective_config_digest = _require_text(
            "effective_config_digest", effective_config_digest
        )
        runtime_lock_digest = _require_text("runtime_lock_digest", runtime_lock_digest)
        if not isinstance(requested_tier, CheckpointTier) or not isinstance(
            minimum_tier, CheckpointTier
        ):
            raise TypeError("requested_tier and minimum_tier must be CheckpointTier members")
        effective_tier = effective_checkpoint_tier(requested_tier, minimum_tier)
        attempt_id = attempt_id or f"attempt-{self._id_factory()}"
        _require_text("attempt_id", attempt_id)
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_lease(connection, lease)
            try:
                connection.execute(
                    """
                    INSERT INTO attempts(
                        instance_id, run_id, attempt_id, fencing_token,
                        plan_digest, effective_config_digest, runtime_lock_digest,
                        requested_tier_name, minimum_tier_name, effective_tier_name,
                        status, started_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?)
                    """,
                    (
                        lease.instance_id,
                        run_id,
                        attempt_id,
                        lease.fencing_token,
                        plan_digest,
                        effective_config_digest,
                        runtime_lock_digest,
                        requested_tier.name,
                        minimum_tier.name,
                        effective_tier.name,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise AttemptConflict(
                    "attempt id already exists or the run already has a running attempt"
                ) from exc
            self._audit(
                connection,
                lease.instance_id,
                "attempt.started",
                attempt_id,
                {
                    "run_id": run_id,
                    "fencing_token": lease.fencing_token,
                    "plan_digest": plan_digest,
                    "effective_config_digest": effective_config_digest,
                    "runtime_lock_digest": runtime_lock_digest,
                    "effective_tier": effective_tier.name,
                },
                now,
            )
        return AttemptHandle(
            instance_id=lease.instance_id,
            run_id=run_id,
            attempt_id=attempt_id,
            fencing_token=lease.fencing_token,
            plan_digest=plan_digest,
            effective_config_digest=effective_config_digest,
            runtime_lock_digest=runtime_lock_digest,
            requested_tier=requested_tier,
            minimum_tier=minimum_tier,
            effective_tier=effective_tier,
            started_at=now,
        )

    def _assert_attempt(
        self,
        connection: sqlite3.Connection,
        lease: Lease,
        attempt: AttemptHandle,
        *,
        allow_committed: bool = False,
    ) -> sqlite3.Row:
        self._assert_lease(connection, lease)
        if lease.instance_id != attempt.instance_id:
            raise FenceRejected("lease and attempt belong to different instances")
        row = connection.execute(
            """
            SELECT * FROM attempts WHERE instance_id = ? AND attempt_id = ?
            """,
            (attempt.instance_id, attempt.attempt_id),
        ).fetchone()
        if row is None:
            raise AttemptConflict("attempt does not exist")
        expected = (
            attempt.run_id,
            attempt.fencing_token,
            attempt.plan_digest,
            attempt.effective_config_digest,
            attempt.runtime_lock_digest,
        )
        durable = (
            row["run_id"],
            row["fencing_token"],
            row["plan_digest"],
            row["effective_config_digest"],
            row["runtime_lock_digest"],
        )
        if expected != durable or attempt.fencing_token != lease.fencing_token:
            raise AttemptConflict("attempt handle does not match its immutable durable binding")
        if row["status"] != "running" and not (
            allow_committed and row["status"] == "committed"
        ):
            raise AttemptConflict(f"attempt is {row['status']}, not running")
        return row

    def recover_attempt(
        self,
        lease: Lease,
        *,
        attempt_id: str,
        plan_digest: str,
        effective_config_digest: str,
        runtime_lock_digest: str,
    ) -> AttemptHandle:
        """Adopt an interrupted attempt under a newer fenced tenure.

        Binding digests cannot change. Effects that may have crossed the external
        boundary are made explicitly ambiguous; recovery must read them back.
        """

        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_lease(connection, lease)
            row = connection.execute(
                "SELECT * FROM attempts WHERE instance_id = ? AND attempt_id = ?",
                (lease.instance_id, attempt_id),
            ).fetchone()
            if row is None or row["status"] != "running":
                raise AttemptConflict("only a running attempt can be recovered")
            if (
                row["plan_digest"],
                row["effective_config_digest"],
                row["runtime_lock_digest"],
            ) != (plan_digest, effective_config_digest, runtime_lock_digest):
                raise AttemptConflict("recovery binding digests differ from the original attempt")
            if row["fencing_token"] >= lease.fencing_token:
                raise AttemptConflict("recovery requires a newer fencing token")
            connection.execute(
                """
                UPDATE attempts SET fencing_token = ?, updated_at = ?
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (lease.fencing_token, now, lease.instance_id, attempt_id),
            )
            connection.execute(
                """
                UPDATE effects
                SET state = 'ambiguous',
                    ambiguity_reason = 'tenure changed after dispatch began',
                    updated_at = ?
                WHERE instance_id = ? AND attempt_id = ? AND state = 'dispatching'
                """,
                (now, lease.instance_id, attempt_id),
            )
            self._audit(
                connection,
                lease.instance_id,
                "attempt.recovered",
                attempt_id,
                {
                    "previous_fencing_token": row["fencing_token"],
                    "new_fencing_token": lease.fencing_token,
                },
                now,
            )
        return AttemptHandle(
            instance_id=lease.instance_id,
            run_id=row["run_id"],
            attempt_id=attempt_id,
            fencing_token=lease.fencing_token,
            plan_digest=plan_digest,
            effective_config_digest=effective_config_digest,
            runtime_lock_digest=runtime_lock_digest,
            requested_tier=_tier_from_name(row["requested_tier_name"]),
            minimum_tier=_tier_from_name(row["minimum_tier_name"]),
            effective_tier=_tier_from_name(row["effective_tier_name"]),
            started_at=row["started_at"],
        )

    def fail_attempt(
        self, lease: Lease, attempt: AttemptHandle, *, reason: str
    ) -> None:
        reason = _require_text("reason", reason)
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            connection.execute(
                """
                UPDATE attempts SET status = 'failed', failure_reason = ?, updated_at = ?
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (reason, now, attempt.instance_id, attempt.attempt_id),
            )
            connection.execute(
                """
                UPDATE effects
                SET state = 'ambiguous', ambiguity_reason = ?, updated_at = ?
                WHERE instance_id = ? AND attempt_id = ? AND state = 'dispatching'
                """,
                ("attempt failed after dispatch began", now, attempt.instance_id, attempt.attempt_id),
            )
            connection.execute(
                """
                UPDATE effects
                SET state = 'cancelled',
                    ambiguity_reason = 'attempt failed before dispatch',
                    updated_at = ?
                WHERE instance_id = ? AND attempt_id = ? AND state = 'prepared'
                """,
                (now, attempt.instance_id, attempt.attempt_id),
            )
            self._audit(
                connection,
                attempt.instance_id,
                "attempt.failed",
                attempt.attempt_id,
                {"reason": reason},
                now,
            )

    def propose_positions(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        positions: Sequence[ProposedPosition],
    ) -> None:
        if len({position.source_id for position in positions}) != len(positions):
            raise AttemptConflict("a source may have only one proposed position per attempt")
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            for position in positions:
                if not isinstance(position, ProposedPosition):
                    raise TypeError("positions must contain ProposedPosition values")
                try:
                    self.comparators.compare(
                        position.position_type,
                        position.encoded_value,
                        position.encoded_value,
                    )
                except ValueError as exc:
                    raise PositionRegression(
                        f"invalid position for source {position.source_id!r}: {exc}"
                    ) from exc
                self._assert_position_advances(connection, attempt.instance_id, position)
                existing = connection.execute(
                    """
                    SELECT * FROM position_proposals
                    WHERE instance_id = ? AND attempt_id = ? AND source_id = ?
                    """,
                    (attempt.instance_id, attempt.attempt_id, position.source_id),
                ).fetchone()
                values = (
                    position.position_type,
                    position.encoded_value,
                    position.coverage_digest,
                )
                if existing is not None:
                    durable = (
                        existing["position_type"],
                        existing["encoded_value"],
                        existing["coverage_digest"],
                    )
                    if values != durable:
                        raise AttemptConflict(
                            f"proposed position for {position.source_id!r} is immutable"
                        )
                    continue
                connection.execute(
                    """
                    INSERT INTO position_proposals(
                        instance_id, attempt_id, source_id, position_type,
                        encoded_value, coverage_digest, proposed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt.instance_id,
                        attempt.attempt_id,
                        position.source_id,
                        *values,
                        now,
                    ),
                )
            self._audit(
                connection,
                attempt.instance_id,
                "positions.proposed",
                attempt.attempt_id,
                tuple(positions),
                now,
            )

    def record_checkpoint(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        *,
        boundary_tier: CheckpointTier,
        boundary_id: str,
        proof: VerificationProof,
    ) -> bool:
        """Persist an eligible immutable checkpoint.

        Returns ``False`` when the boundary is finer than the effective tier, so
        an Anchor executor can use one call for every potential boundary.
        """

        if not isinstance(boundary_tier, CheckpointTier):
            raise TypeError("boundary_tier must be a CheckpointTier")
        if not checkpoint_is_enabled(attempt.effective_tier, boundary_tier):
            return False
        boundary_id = _require_text("boundary_id", boundary_id)
        if not isinstance(proof, VerificationProof) or not _proof_is_verified(proof):
            raise CheckpointConflict("a checkpoint requires a verified VerificationProof")
        proof_digest = canonical_digest(proof)
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            row = connection.execute(
                """
                SELECT proof_id, proof_digest FROM checkpoints
                WHERE instance_id = ? AND attempt_id = ?
                  AND boundary_tier_name = ? AND boundary_id = ?
                """,
                (
                    attempt.instance_id,
                    attempt.attempt_id,
                    boundary_tier.name,
                    boundary_id,
                ),
            ).fetchone()
            if row is not None:
                if row["proof_id"] != proof.proof_id or row["proof_digest"] != proof_digest:
                    raise CheckpointConflict("checkpoint evidence is immutable")
                return True
            connection.execute(
                """
                INSERT INTO checkpoints(
                    instance_id, attempt_id, boundary_tier_name, boundary_id,
                    proof_id, proof_digest, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt.instance_id,
                    attempt.attempt_id,
                    boundary_tier.name,
                    boundary_id,
                    proof.proof_id,
                    proof_digest,
                    now,
                ),
            )
            self._audit(
                connection,
                attempt.instance_id,
                "checkpoint.recorded",
                boundary_id,
                {"tier": boundary_tier.name, "proof_digest": proof_digest},
                now,
            )
        return True

    def list_checkpoints(
        self, instance_id: str, attempt_id: str
    ) -> tuple[CheckpointRecord, ...]:
        """Return immutable checkpoint facts for an executor resuming an attempt."""

        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            attempt_row = connection.execute(
                """
                SELECT 1 FROM attempts WHERE instance_id = ? AND attempt_id = ?
                """,
                (instance_id, attempt_id),
            ).fetchone()
            if attempt_row is None:
                raise AttemptConflict("attempt does not exist")
            return tuple(
                CheckpointRecord(
                    instance_id=row["instance_id"],
                    attempt_id=row["attempt_id"],
                    boundary_tier=_tier_from_name(row["boundary_tier_name"]),
                    boundary_id=row["boundary_id"],
                    proof_id=row["proof_id"],
                    proof_digest=row["proof_digest"],
                    recorded_at=row["recorded_at"],
                )
                for row in connection.execute(
                    """
                    SELECT * FROM checkpoints
                    WHERE instance_id = ? AND attempt_id = ?
                    ORDER BY recorded_at, boundary_tier_name, boundary_id
                    """,
                    (instance_id, attempt_id),
                )
            )

    def list_proposed_positions(
        self, instance_id: str, attempt_id: str
    ) -> tuple[ProposedPosition, ...]:
        """Return durable proposals without confusing them with committed state."""

        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            attempt_row = connection.execute(
                """
                SELECT 1 FROM attempts WHERE instance_id = ? AND attempt_id = ?
                """,
                (instance_id, attempt_id),
            ).fetchone()
            if attempt_row is None:
                raise AttemptConflict("attempt does not exist")
            return tuple(
                ProposedPosition(
                    source_id=row["source_id"],
                    position_type=row["position_type"],
                    encoded_value=row["encoded_value"],
                    coverage_digest=row["coverage_digest"],
                )
                for row in connection.execute(
                    """
                    SELECT * FROM position_proposals
                    WHERE instance_id = ? AND attempt_id = ? ORDER BY source_id
                    """,
                    (instance_id, attempt_id),
                )
            )

    def prepare_effect(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        *,
        effect_id: str,
        provider: str,
        capability: str,
        operation_digest: str,
        idempotency_key: str,
    ) -> EffectIntent:
        values = tuple(
            _require_text(name, value)
            for name, value in (
                ("effect_id", effect_id),
                ("provider", provider),
                ("capability", capability),
                ("operation_digest", operation_digest),
                ("idempotency_key", idempotency_key),
            )
        )
        effect_id, provider, capability, operation_digest, idempotency_key = values
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            row = connection.execute(
                """
                SELECT * FROM effects WHERE instance_id = ? AND effect_id = ?
                """,
                (attempt.instance_id, effect_id),
            ).fetchone()
            if row is not None:
                immutable = (
                    row["attempt_id"],
                    row["provider"],
                    row["capability"],
                    row["operation_digest"],
                    row["idempotency_key"],
                )
                expected = (
                    attempt.attempt_id,
                    provider,
                    capability,
                    operation_digest,
                    idempotency_key,
                )
                if immutable != expected:
                    raise EffectConflict("effect identity is already bound to another operation")
                return self._effect_from_row(row)
            try:
                connection.execute(
                    """
                    INSERT INTO effects(
                        instance_id, attempt_id, effect_id, provider, capability,
                        operation_digest, idempotency_key, state, prepared_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?)
                    """,
                    (
                        attempt.instance_id,
                        attempt.attempt_id,
                        effect_id,
                        provider,
                        capability,
                        operation_digest,
                        idempotency_key,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise EffectConflict("idempotency key is already bound to another effect") from exc
            self._audit(
                connection,
                attempt.instance_id,
                "effect.prepared",
                effect_id,
                {
                    "provider": provider,
                    "capability": capability,
                    "operation_digest": operation_digest,
                    "idempotency_key": idempotency_key,
                },
                now,
            )
        return EffectIntent(
            instance_id=attempt.instance_id,
            attempt_id=attempt.attempt_id,
            effect_id=effect_id,
            provider=provider,
            capability=capability,
            operation_digest=operation_digest,
            idempotency_key=idempotency_key,
            state="prepared",
            prepared_at=now,
            updated_at=now,
        )

    def mark_effect_dispatching(
        self, lease: Lease, attempt: AttemptHandle, *, effect_id: str
    ) -> EffectIntent:
        """Durably cross the pre-dispatch boundary before invoking external code."""

        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            row = self._require_effect(connection, attempt.instance_id, effect_id)
            if row["attempt_id"] != attempt.attempt_id:
                raise EffectConflict("effect belongs to another attempt")
            if row["state"] == "dispatching":
                raise ReadbackRequired(
                    "effect already crossed the dispatch boundary; verify read-back before retry"
                )
            if row["state"] in ("ambiguous", "verified", "cancelled"):
                raise EffectConflict(f"cannot dispatch an effect in state {row['state']!r}")
            if row["state"] == "prepared":
                connection.execute(
                    """
                    UPDATE effects SET state = 'dispatching', updated_at = ?
                    WHERE instance_id = ? AND effect_id = ?
                    """,
                    (now, attempt.instance_id, effect_id),
                )
                self._audit(
                    connection,
                    attempt.instance_id,
                    "effect.dispatching",
                    effect_id,
                    {"operation_digest": row["operation_digest"]},
                    now,
                )
                row = dict(row)
                row["state"] = "dispatching"
                row["updated_at"] = now
            return self._effect_from_row(row)

    def mark_effect_ambiguous(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        *,
        effect_id: str,
        reason: str,
    ) -> EffectIntent:
        reason = _require_text("reason", reason)
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            row = self._require_effect(connection, attempt.instance_id, effect_id)
            if row["attempt_id"] != attempt.attempt_id or row["state"] not in (
                "dispatching",
                "ambiguous",
            ):
                raise EffectConflict("only a dispatching effect can become ambiguous")
            if row["state"] == "ambiguous" and row["ambiguity_reason"] != reason:
                raise EffectConflict("an ambiguity reason is immutable once recorded")
            connection.execute(
                """
                UPDATE effects
                SET state = 'ambiguous', ambiguity_reason = ?, updated_at = ?
                WHERE instance_id = ? AND effect_id = ?
                """,
                (reason, now, attempt.instance_id, effect_id),
            )
            self._audit(
                connection,
                attempt.instance_id,
                "effect.ambiguous",
                effect_id,
                {"reason": reason},
                now,
            )
            row = dict(row)
            row["state"] = "ambiguous"
            row["updated_at"] = now
            row["ambiguity_reason"] = reason
            return self._effect_from_row(row)

    def record_effect_receipt(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        *,
        receipt: EffectReceipt,
        proof: VerificationProof,
    ) -> EffectIntent:
        if not isinstance(receipt, EffectReceipt):
            raise TypeError("receipt must be an EffectReceipt")
        if not isinstance(proof, VerificationProof) or not _proof_is_verified(proof):
            raise EffectConflict("effect read-back requires a verified VerificationProof")
        if receipt.verification_proof_id != proof.proof_id:
            raise EffectConflict("receipt references a different verification proof")
        if not receipt.readback_digest:
            raise EffectConflict("effect receipt has no read-back digest")
        if proof.subject_digest != receipt.readback_digest:
            raise EffectConflict("verification proof does not bind the receipt read-back digest")
        now = _rfc3339(self._clock())
        receipt_json = canonical_json(receipt)
        proof_json = canonical_json(proof)
        proof_digest = canonical_digest(proof)
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            row = self._require_effect(connection, attempt.instance_id, receipt.effect_id)
            if row["attempt_id"] != attempt.attempt_id:
                raise EffectConflict("effect belongs to another attempt")
            expected = (row["provider"], row["capability"], row["operation_digest"])
            actual = (receipt.provider, receipt.capability, receipt.operation_digest)
            if expected != actual:
                raise EffectConflict("receipt does not match the durable effect intent")
            if row["state"] == "prepared":
                raise EffectConflict("an effect cannot be verified before dispatch begins")
            if row["state"] == "cancelled":
                raise EffectConflict("a cancelled effect cannot be verified")
            if row["state"] == "verified":
                if row["receipt_json"] != receipt_json or row["proof_digest"] != proof_digest:
                    raise EffectConflict("verified effect evidence is immutable")
                return self._effect_from_row(row)
            connection.execute(
                """
                UPDATE effects SET
                    state = 'verified', updated_at = ?, readback_digest = ?,
                    verification_proof_id = ?, proof_digest = ?,
                    receipt_json = ?, proof_json = ?, ambiguity_reason = NULL
                WHERE instance_id = ? AND effect_id = ?
                """,
                (
                    now,
                    receipt.readback_digest,
                    proof.proof_id,
                    proof_digest,
                    receipt_json,
                    proof_json,
                    attempt.instance_id,
                    receipt.effect_id,
                ),
            )
            self._audit(
                connection,
                attempt.instance_id,
                "effect.verified",
                receipt.effect_id,
                {
                    "readback_digest": receipt.readback_digest,
                    "proof_digest": proof_digest,
                },
                now,
            )
            row = dict(row)
            row["state"] = "verified"
            row["updated_at"] = now
            return self._effect_from_row(row)

    def _require_effect(
        self, connection: sqlite3.Connection, instance_id: str, effect_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM effects WHERE instance_id = ? AND effect_id = ?",
            (instance_id, effect_id),
        ).fetchone()
        if row is None:
            raise EffectConflict(f"unknown effect {effect_id!r}")
        return row

    @staticmethod
    def _effect_from_row(row: sqlite3.Row | dict[str, Any]) -> EffectIntent:
        return EffectIntent(
            instance_id=row["instance_id"],
            attempt_id=row["attempt_id"],
            effect_id=row["effect_id"],
            provider=row["provider"],
            capability=row["capability"],
            operation_digest=row["operation_digest"],
            idempotency_key=row["idempotency_key"],
            state=cast(Any, row["state"]),
            prepared_at=row["prepared_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _effect_recovery_from_row(row: sqlite3.Row) -> EffectRecovery:
        effect = SQLiteRatchet._effect_from_row(row)
        if effect.state == "prepared":
            action = "safe_to_dispatch"
            reason = None
        elif effect.state in ("dispatching", "ambiguous"):
            action = "readback_required"
            reason = row["ambiguity_reason"] or "dispatch outcome has not been verified"
        else:
            action = "no_action"
            reason = row["ambiguity_reason"] if effect.state == "cancelled" else None
        return EffectRecovery(effect=effect, action=cast(Any, action), reason=reason)

    def effect_recovery(self, instance_id: str, effect_id: str) -> EffectRecovery:
        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            return self._effect_recovery_from_row(
                self._require_effect(connection, instance_id, effect_id)
            )

    def _validate_bundle(
        self,
        connection: sqlite3.Connection,
        attempt: AttemptHandle,
        bundle: RunCommitBundle,
        cohorts: Sequence[CommitCohort],
        required_proof_types: frozenset[str],
    ) -> None:
        if not isinstance(bundle, RunCommitBundle):
            raise TypeError("bundle must be a RunCommitBundle")
        if any(not isinstance(cohort, CommitCohort) for cohort in cohorts):
            raise TypeError("cohorts must contain CommitCohort values")
        identity = (bundle.instance_id, bundle.run_id, bundle.attempt_id)
        if identity != (attempt.instance_id, attempt.run_id, attempt.attempt_id):
            raise BundleRejected("bundle identity does not match the attempt")
        bindings = (
            bundle.plan_digest,
            bundle.effective_config_digest,
            bundle.runtime_lock_digest,
        )
        if bindings != (
            attempt.plan_digest,
            attempt.effective_config_digest,
            attempt.runtime_lock_digest,
        ):
            raise BundleRejected("bundle binding digests do not match the attempt")
        for field_name in (
            "candidate_snapshot_digest",
            "disposition_digest",
            "cohort_manifest_digest",
        ):
            if not getattr(bundle, field_name):
                raise BundleRejected(f"bundle has no {field_name}")
        if bundle.cohort_manifest_digest != canonical_digest(tuple(cohorts)):
            raise BundleRejected("cohort manifest digest does not match supplied cohorts")

        positions = tuple(bundle.proposed_positions)
        proofs = tuple(bundle.proofs)
        receipts = tuple(bundle.effect_receipts)
        if len(set(bundle.coverage_digests)) != len(bundle.coverage_digests):
            raise BundleRejected("bundle repeats a coverage digest")
        if len(set(bundle.exception_ids)) != len(bundle.exception_ids):
            raise BundleRejected("bundle repeats an exception id")
        # V2-EXC-005: hard safety is non-waivable, and Ratchet owns those
        # invariants, so Ratchet refuses the waiver rather than trusting Core to
        # have filtered it. The bundle declares the rule ids its exceptions
        # waive; matching against opaque exception ids would never fire.
        waived_safety = sorted(set(bundle.waived_rule_ids) & NON_WAIVABLE_REASON_CODES)
        if waived_safety:
            raise BundleRejected(
                f"bundle attempts to waive non-waivable safety: {waived_safety}"
            )
        if bundle.exception_ids and not bundle.waived_rule_ids:
            raise BundleRejected(
                "bundle carries exceptions without declaring which rules they waive"
            )
        if len({position.source_id for position in positions}) != len(positions):
            raise BundleRejected("bundle repeats a proposed source position")
        if len({proof.proof_id for proof in proofs}) != len(proofs):
            raise BundleRejected("bundle repeats a proof id")
        if len({receipt.effect_id for receipt in receipts}) != len(receipts):
            raise BundleRejected("bundle repeats an effect receipt")
        if not proofs:
            raise BundleRejected("bundle has no verification proofs")
        if any(not isinstance(proof, VerificationProof) for proof in proofs):
            raise BundleRejected("bundle contains an untyped proof")
        if any(not _proof_is_verified(proof) for proof in proofs):
            raise BundleRejected("bundle contains an unverified proof")

        proof_types = {proof.proof_type for proof in proofs}
        missing_types = required_proof_types - proof_types
        if missing_types:
            raise BundleRejected(
                f"bundle is missing required proof types: {sorted(missing_types)!r}"
            )
        proof_subjects = {proof.subject_digest for proof in proofs}
        structurally_verified = {
            bundle.candidate_snapshot_digest,
            bundle.disposition_digest,
            bundle.cohort_manifest_digest,
            *bundle.coverage_digests,
        }
        missing_subjects = structurally_verified - proof_subjects
        if missing_subjects:
            raise BundleRejected(
                "bundle lacks verified proof subjects for structural digests: "
                f"{sorted(missing_subjects)!r}"
            )

        coverage_digests = set(bundle.coverage_digests)
        for position in positions:
            if not isinstance(position, ProposedPosition):
                raise BundleRejected("bundle contains an untyped proposed position")
            if position.coverage_digest not in coverage_digests:
                raise BundleRejected(
                    f"position {position.source_id!r} references absent coverage"
                )
            durable = connection.execute(
                """
                SELECT position_type, encoded_value, coverage_digest
                FROM position_proposals
                WHERE instance_id = ? AND attempt_id = ? AND source_id = ?
                """,
                (attempt.instance_id, attempt.attempt_id, position.source_id),
            ).fetchone()
            if durable is None or (
                durable["position_type"],
                durable["encoded_value"],
                durable["coverage_digest"],
            ) != (
                position.position_type,
                position.encoded_value,
                position.coverage_digest,
            ):
                raise BundleRejected(
                    f"bundle position {position.source_id!r} was not durably proposed"
                )

        bundle_sources = {position.source_id for position in positions}
        durable_proposal_sources = {
            row["source_id"]
            for row in connection.execute(
                """
                SELECT source_id FROM position_proposals
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (attempt.instance_id, attempt.attempt_id),
            )
        }
        if bundle_sources != durable_proposal_sources:
            raise BundleRejected(
                "bundle must include every durable proposal and no unproposed source"
            )
        bundle_outputs = {proof.proof_id for proof in proofs} | proof_subjects
        bundle_effects = {receipt.effect_id for receipt in receipts}
        seen_cohort_ids: set[str] = set()
        seen_sources: set[str] = set()
        seen_outputs: set[str] = set()
        seen_effects: set[str] = set()
        for cohort in cohorts:
            if cohort.cohort_id in seen_cohort_ids:
                raise BundleRejected(f"duplicate cohort id {cohort.cohort_id!r}")
            seen_cohort_ids.add(cohort.cohort_id)
            if (
                len(set(cohort.source_ids)) != len(cohort.source_ids)
                or len(set(cohort.output_ids)) != len(cohort.output_ids)
                or len(set(cohort.effect_ids)) != len(cohort.effect_ids)
            ):
                raise BundleRejected(
                    f"commit cohort {cohort.cohort_id!r} repeats a member"
                )
            overlapping = (
                seen_sources & set(cohort.source_ids),
                seen_outputs & set(cohort.output_ids),
                seen_effects & set(cohort.effect_ids),
            )
            if any(overlapping):
                raise BundleRejected(
                    "commit cohort members must be disjoint; merge dependent cohorts"
                )
            seen_sources.update(cohort.source_ids)
            seen_outputs.update(cohort.output_ids)
            seen_effects.update(cohort.effect_ids)
            declared = set(cohort.source_ids) | set(cohort.output_ids) | set(cohort.effect_ids)
            present = (
                set(cohort.source_ids) & bundle_sources
            ) | (set(cohort.output_ids) & bundle_outputs) | (
                set(cohort.effect_ids) & bundle_effects
            )
            if present and (
                not set(cohort.source_ids).issubset(bundle_sources)
                or not set(cohort.output_ids).issubset(bundle_outputs)
                or not set(cohort.effect_ids).issubset(bundle_effects)
            ):
                raise CohortIncomplete(
                    f"commit cohort {cohort.cohort_id!r} is only partially present"
                )
            if not declared:
                raise BundleRejected(f"commit cohort {cohort.cohort_id!r} is empty")

        if not bundle_sources.issubset(seen_sources):
            raise BundleRejected("every proposed source must belong to a commit cohort")
        if not bundle_effects.issubset(seen_effects):
            raise BundleRejected("every effect receipt must belong to a commit cohort")
        if bundle.candidate_snapshot_digest not in seen_outputs:
            raise BundleRejected("candidate snapshot must belong to a commit cohort")

        proof_by_id = {proof.proof_id: proof for proof in proofs}
        durable_effects = connection.execute(
            "SELECT * FROM effects WHERE instance_id = ? AND attempt_id = ?",
            (attempt.instance_id, attempt.attempt_id),
        ).fetchall()
        receipt_by_id = {receipt.effect_id: receipt for receipt in receipts}
        if set(receipt_by_id) != {row["effect_id"] for row in durable_effects}:
            raise BundleRejected("bundle must include every effect prepared by the attempt")
        for row in durable_effects:
            if row["state"] != "verified":
                raise BundleRejected(
                    f"effect {row['effect_id']!r} has no verified read-back"
                )
            receipt = receipt_by_id[row["effect_id"]]
            proof = proof_by_id.get(receipt.verification_proof_id)
            if proof is None:
                raise BundleRejected(
                    f"effect {row['effect_id']!r} references a proof outside the bundle"
                )
            if (
                row["receipt_json"] != canonical_json(receipt)
                or row["proof_digest"] != canonical_digest(proof)
                or row["readback_digest"] != receipt.readback_digest
            ):
                raise BundleRejected(
                    f"effect {row['effect_id']!r} evidence differs from durable read-back"
                )

    def stage_bundle(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        bundle: RunCommitBundle,
        *,
        cohorts: Sequence[CommitCohort],
        required_proof_types: Iterable[str] = (),
    ) -> str:
        required = frozenset(
            _require_text("required proof type", value) for value in required_proof_types
        )
        bundle_digest = bundle.digest
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_attempt(connection, lease, attempt)
            self._validate_bundle(connection, attempt, bundle, cohorts, required)
            row = connection.execute(
                """
                SELECT bundle_digest, required_proof_types_json, cohorts_json
                FROM staged_bundles
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (attempt.instance_id, attempt.attempt_id),
            ).fetchone()
            if row is not None:
                if (
                    row["bundle_digest"] != bundle_digest
                    or row["required_proof_types_json"]
                    != canonical_json(tuple(sorted(required)))
                    or row["cohorts_json"] != canonical_json(tuple(cohorts))
                ):
                    raise AlreadyCommitted("a different immutable bundle is already staged")
                return bundle_digest
            connection.execute(
                """
                INSERT INTO staged_bundles(
                    instance_id, attempt_id, bundle_digest, bundle_json,
                    required_proof_types_json, cohorts_json, staged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt.instance_id,
                    attempt.attempt_id,
                    bundle_digest,
                    canonical_json(bundle),
                    canonical_json(tuple(sorted(required))),
                    canonical_json(tuple(cohorts)),
                    now,
                ),
            )
            self._audit(
                connection,
                attempt.instance_id,
                "bundle.staged",
                attempt.attempt_id,
                {"bundle_digest": bundle_digest},
                now,
            )
        return bundle_digest

    def commit_bundle(
        self,
        lease: Lease,
        attempt: AttemptHandle,
        bundle: RunCommitBundle,
        *,
        cohorts: Sequence[CommitCohort],
        outcome: TerminalOutcome,
        required_proof_types: Iterable[str] = (),
    ) -> CommitCertificate:
        """Atomically certify a bundle, advance positions, and activate its snapshot."""

        if not isinstance(outcome, TerminalOutcome):
            raise TypeError("outcome must be a TerminalOutcome")
        if outcome not in {
            TerminalOutcome.COMPLETED, TerminalOutcome.COMPLETED_WITH_EXCEPTIONS,
            TerminalOutcome.NO_CHANGE,
        }:
            raise BundleRejected("a failed, gated, or abandoned attempt cannot commit")
        required = frozenset(
            _require_text("required proof type", value) for value in required_proof_types
        )
        bundle_digest = bundle.digest
        now = _rfc3339(self._clock())
        with self._transaction(write=True) as connection:
            self._assert_lease(connection, lease)
            if lease.instance_id != attempt.instance_id:
                raise FenceRejected("lease and attempt belong to different instances")
            existing = connection.execute(
                """
                SELECT * FROM certificates
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (attempt.instance_id, attempt.attempt_id),
            ).fetchone()
            if existing is not None:
                if existing["bundle_digest"] != bundle_digest:
                    raise AlreadyCommitted("attempt already committed a different bundle")
                staged = connection.execute(
                    "SELECT required_proof_types_json, cohorts_json FROM staged_bundles "
                    "WHERE instance_id = ? AND attempt_id = ?",
                    (attempt.instance_id, attempt.attempt_id),
                ).fetchone()
                if (existing["outcome_name"] != outcome.name or staged is None
                        or staged["required_proof_types_json"] != canonical_json(tuple(sorted(required)))
                        or staged["cohorts_json"] != canonical_json(tuple(cohorts))):
                    raise AlreadyCommitted("commit replay changes outcome or verification policy")
                return self._certificate_from_row(existing)
            self._assert_attempt(connection, lease, attempt)
            self._validate_bundle(connection, attempt, bundle, cohorts, required)
            staged = connection.execute(
                """
                SELECT * FROM staged_bundles
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (attempt.instance_id, attempt.attempt_id),
            ).fetchone()
            expected_required_json = canonical_json(tuple(sorted(required)))
            expected_cohorts_json = canonical_json(tuple(cohorts))
            if staged is None:
                connection.execute(
                    """
                    INSERT INTO staged_bundles(
                        instance_id, attempt_id, bundle_digest, bundle_json,
                        required_proof_types_json, cohorts_json, staged_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt.instance_id,
                        attempt.attempt_id,
                        bundle_digest,
                        canonical_json(bundle),
                        expected_required_json,
                        expected_cohorts_json,
                        now,
                    ),
                )
            elif (
                staged["bundle_digest"] != bundle_digest
                or staged["required_proof_types_json"] != expected_required_json
                or staged["cohorts_json"] != expected_cohorts_json
            ):
                raise AlreadyCommitted("commit differs from the immutable staged bundle")

            position_payload = tuple(
                sorted(
                    (
                        position.source_id,
                        position.position_type,
                        position.encoded_value,
                        position.coverage_digest,
                    )
                    for position in bundle.proposed_positions
                )
            )
            committed_position_digest = canonical_digest(position_payload)
            active = connection.execute(
                """
                SELECT c.certificate_digest
                FROM active_snapshots AS a
                JOIN certificates AS c
                  ON c.instance_id = a.instance_id
                 AND c.certificate_id = a.certificate_id
                WHERE a.instance_id = ?
                """,
                (attempt.instance_id,),
            ).fetchone()
            previous_digest = active["certificate_digest"] if active is not None else None
            certificate = CommitCertificate(
                certificate_id=f"certificate-{self._id_factory()}",
                instance_id=attempt.instance_id,
                run_id=attempt.run_id,
                attempt_id=attempt.attempt_id,
                bundle_digest=bundle_digest,
                committed_at=now,
                outcome=outcome,
                active_snapshot_digest=bundle.candidate_snapshot_digest,
                committed_position_digest=committed_position_digest,
                previous_certificate_digest=previous_digest,
            )
            certificate_digest = canonical_digest(certificate)
            connection.execute(
                """
                INSERT INTO certificates(
                    instance_id, certificate_id, run_id, attempt_id, bundle_digest,
                    committed_at, outcome_name, active_snapshot_digest,
                    committed_position_digest, previous_certificate_digest,
                    certificate_digest, certificate_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    certificate.instance_id,
                    certificate.certificate_id,
                    certificate.run_id,
                    certificate.attempt_id,
                    certificate.bundle_digest,
                    certificate.committed_at,
                    certificate.outcome.name,
                    certificate.active_snapshot_digest,
                    certificate.committed_position_digest,
                    certificate.previous_certificate_digest,
                    certificate_digest,
                    canonical_json(certificate),
                ),
            )
            for position in bundle.proposed_positions:
                # V2-RATCHET-004: re-check monotonicity *inside* the commit
                # transaction. Checking only at proposal time leaves a real
                # window: a lapsed attempt that proposed P can be recovered
                # under a newer fencing token and committed after a different
                # run already committed a position ahead of P. Proposal-time
                # state is stale by then; only this read is authoritative.
                self._assert_position_advances(connection, attempt.instance_id, position)
                connection.execute(
                    """
                    INSERT INTO committed_positions(
                        instance_id, source_id, position_type, encoded_value,
                        coverage_digest, certificate_id, committed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instance_id, source_id) DO UPDATE SET
                        position_type = excluded.position_type,
                        encoded_value = excluded.encoded_value,
                        coverage_digest = excluded.coverage_digest,
                        certificate_id = excluded.certificate_id,
                        committed_at = excluded.committed_at
                    """,
                    (
                        attempt.instance_id,
                        position.source_id,
                        position.position_type,
                        position.encoded_value,
                        position.coverage_digest,
                        certificate.certificate_id,
                        now,
                    ),
                )
            connection.execute(
                """
                INSERT INTO active_snapshots(
                    instance_id, snapshot_digest, certificate_id, activated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    snapshot_digest = excluded.snapshot_digest,
                    certificate_id = excluded.certificate_id,
                    activated_at = excluded.activated_at
                """,
                (
                    attempt.instance_id,
                    bundle.candidate_snapshot_digest,
                    certificate.certificate_id,
                    now,
                ),
            )
            connection.execute(
                """
                UPDATE attempts SET status = 'committed', updated_at = ?
                WHERE instance_id = ? AND attempt_id = ?
                """,
                (now, attempt.instance_id, attempt.attempt_id),
            )
            self._audit(
                connection,
                attempt.instance_id,
                "bundle.committed",
                certificate.certificate_id,
                {
                    "bundle_digest": bundle_digest,
                    "certificate_digest": certificate_digest,
                    "snapshot_digest": bundle.candidate_snapshot_digest,
                },
                now,
            )
            return certificate

    @staticmethod
    def _certificate_from_row(row: sqlite3.Row) -> CommitCertificate:
        return CommitCertificate(
            certificate_id=row["certificate_id"],
            instance_id=row["instance_id"],
            run_id=row["run_id"],
            attempt_id=row["attempt_id"],
            bundle_digest=row["bundle_digest"],
            committed_at=row["committed_at"],
            outcome=_outcome_from_name(row["outcome_name"]),
            active_snapshot_digest=row["active_snapshot_digest"],
            committed_position_digest=row["committed_position_digest"],
            previous_certificate_digest=row["previous_certificate_digest"],
        )

    def get_certificate(self, instance_id: str, certificate_id: str) -> CommitCertificate:
        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            row = connection.execute(
                """
                SELECT * FROM certificates
                WHERE instance_id = ? AND certificate_id = ?
                """,
                (instance_id, certificate_id),
            ).fetchone()
            if row is None:
                raise BundleRejected(f"unknown certificate {certificate_id!r}")
            return self._certificate_from_row(row)

    def certificate_chain(self, instance_id: str) -> tuple[CommitCertificate, ...]:
        """Return and verify the linear certificate chain, root first.

        V2-RATCHET-011 requires an inspectable chain rather than a link that is
        merely written and never checked. Every stored certificate must belong
        to the active chain; missing links, altered certificate fields, cycles,
        forks, and orphaned certificates fail closed.
        """

        instance_id = _require_text("instance_id", instance_id)
        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            rows = connection.execute(
                """
                SELECT * FROM certificates
                WHERE instance_id = ?
                ORDER BY committed_at, certificate_id
                """,
                (instance_id,),
            ).fetchall()
            active = connection.execute(
                """
                SELECT certificate_id FROM active_snapshots
                WHERE instance_id = ?
                """,
                (instance_id,),
            ).fetchone()
            if not rows:
                if active is not None:
                    raise BundleRejected("active snapshot references an absent certificate")
                return ()
            if active is None:
                raise BundleRejected("certificate history exists without an active snapshot")

            by_id = {row["certificate_id"]: row for row in rows}
            by_digest: dict[str, sqlite3.Row] = {}
            successors: dict[str | None, str] = {}
            for row in rows:
                certificate = self._certificate_from_row(row)
                actual_digest = canonical_digest(certificate)
                if actual_digest != row["certificate_digest"]:
                    raise BundleRejected(
                        f"certificate {certificate.certificate_id!r} digest verification failed"
                    )
                if actual_digest in by_digest:
                    raise BundleRejected("duplicate certificate digest in chain")
                by_digest[actual_digest] = row
                previous = certificate.previous_certificate_digest
                if previous in successors:
                    raise BundleRejected(
                        f"certificate chain forks after {previous or '<root>'!r}"
                    )
                successors[previous] = certificate.certificate_id

            current = by_id.get(active["certificate_id"])
            if current is None:
                raise BundleRejected("active snapshot references an unknown certificate")
            reversed_chain: list[CommitCertificate] = []
            visited: set[str] = set()
            while current is not None:
                certificate = self._certificate_from_row(current)
                if certificate.certificate_id in visited:
                    raise BundleRejected("certificate chain contains a cycle")
                visited.add(certificate.certificate_id)
                reversed_chain.append(certificate)
                previous = certificate.previous_certificate_digest
                if previous is None:
                    break
                current = by_digest.get(previous)
                if current is None:
                    raise BundleRejected(
                        f"certificate {certificate.certificate_id!r} has a missing predecessor"
                    )

            if len(visited) != len(rows):
                raise BundleRejected("certificate history contains an orphaned branch")
            return tuple(reversed(reversed_chain))

    @staticmethod
    def _active_snapshot(connection: sqlite3.Connection, instance_id: str) -> ActiveSnapshot | None:
        row = connection.execute(
            "SELECT * FROM active_snapshots WHERE instance_id = ?", (instance_id,)
        ).fetchone()
        if row is None:
            return None
        return ActiveSnapshot(
            instance_id=row["instance_id"],
            snapshot_digest=row["snapshot_digest"],
            certificate_id=row["certificate_id"],
            activated_at=row["activated_at"],
        )

    @staticmethod
    def _attempt_summary(row: sqlite3.Row) -> AttemptSummary:
        return AttemptSummary(
            instance_id=row["instance_id"],
            run_id=row["run_id"],
            attempt_id=row["attempt_id"],
            status=row["status"],
            fencing_token=row["fencing_token"],
            effective_tier=_tier_from_name(row["effective_tier_name"]),
            started_at=row["started_at"],
            updated_at=row["updated_at"],
            failure_reason=row["failure_reason"],
        )

    @staticmethod
    def _lease_status(
        connection: sqlite3.Connection, instance_id: str, now_epoch: float
    ) -> Lease | None:
        row = connection.execute(
            "SELECT * FROM leases WHERE instance_id = ?", (instance_id,)
        ).fetchone()
        if row is None or row["expires_epoch"] <= now_epoch:
            return None
        return Lease(
            instance_id=row["instance_id"],
            owner_id=row["owner_id"],
            lease_id=row["lease_id"],
            fencing_token=row["fencing_token"],
            acquired_at=row["acquired_at"],
            expires_at=row["expires_at"],
        )

    def status(self, instance_id: str) -> InstanceStatus:
        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            positions = tuple(
                CommittedPosition(
                    source_id=row["source_id"],
                    position_type=row["position_type"],
                    encoded_value=row["encoded_value"],
                    coverage_digest=row["coverage_digest"],
                    certificate_id=row["certificate_id"],
                    committed_at=row["committed_at"],
                )
                for row in connection.execute(
                    """
                    SELECT * FROM committed_positions
                    WHERE instance_id = ? ORDER BY source_id
                    """,
                    (instance_id,),
                )
            )
            attempts = tuple(
                self._attempt_summary(row)
                for row in connection.execute(
                    """
                    SELECT * FROM attempts
                    WHERE instance_id = ? ORDER BY started_at, attempt_id
                    """,
                    (instance_id,),
                )
            )
            effects = tuple(
                self._effect_recovery_from_row(row)
                for row in connection.execute(
                    """
                    SELECT * FROM effects
                    WHERE instance_id = ? AND state NOT IN ('verified', 'cancelled')
                    ORDER BY prepared_at, effect_id
                    """,
                    (instance_id,),
                )
            )
            return InstanceStatus(
                instance_id=instance_id,
                current_lease=self._lease_status(connection, instance_id, self._clock()),
                active_snapshot=self._active_snapshot(connection, instance_id),
                committed_positions=positions,
                attempts=attempts,
                unresolved_effects=effects,
            )

    def recovery_status(self, instance_id: str) -> RecoveryStatus:
        """Return only facts needed to choose safe restart actions."""

        with self._transaction(write=False) as connection:
            self._require_instance(connection, instance_id)
            attempts = tuple(
                self._attempt_summary(row)
                for row in connection.execute(
                    """
                    SELECT * FROM attempts
                    WHERE instance_id = ? AND status IN ('running', 'failed')
                    ORDER BY started_at, attempt_id
                    """,
                    (instance_id,),
                )
            )
            effects = tuple(
                self._effect_recovery_from_row(row)
                for row in connection.execute(
                    """
                    SELECT * FROM effects
                    WHERE instance_id = ? ORDER BY prepared_at, effect_id
                    """,
                    (instance_id,),
                )
            )
            return RecoveryStatus(
                instance_id=instance_id,
                active_snapshot=self._active_snapshot(connection, instance_id),
                attempts=attempts,
                effects=effects,
            )
