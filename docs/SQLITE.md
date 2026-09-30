# Transactional SQLite API — 0.6.0a1

`ratchet_sqlite.SQLiteRatchet` is a local, synchronous integrity kernel for a host
that already owns scheduling and execution. It uses WAL, full synchronous writes,
foreign keys, and `BEGIN IMMEDIATE` to check fencing in the same transaction as
mutation. Python 3.11+ and the standard library are sufficient.

## Use

```python
from ratchet_sqlite import SQLiteRatchet
from ratchet_sqlite.contracts import CheckpointTier, canonical_digest

runtime = SQLiteRatchet("state.sqlite3")
runtime.initialize_instance("example")
lease = runtime.acquire("example", "worker", ttl_seconds=60)
attempt = runtime.begin_attempt(
    lease, run_id="run-001",
    plan_digest=canonical_digest({"workflow": "example", "version": 1}),
    effective_config_digest=canonical_digest({"mode": "full"}),
    runtime_lock_digest=canonical_digest({"host": "example-v1"}),
    requested_tier=CheckpointTier.PHASE, minimum_tier=CheckpointTier.RUN,
)
# Execute and verify work, renew before expiration, then commit or fail the attempt.
runtime.fail_attempt(lease, attempt, reason="demonstration stopped before work")
runtime.release(lease)
```

Run `python examples/sqlite_commit.py` for a complete synthetic commit, position
advance, certificate replay and read-only inspection. Install the package first.

## Contract

- Every mutation after acquisition checks a current lease. A replacement owner
  receives a larger fencing token. The host must renew leases; there is no
  background heartbeat in this API.
- An attempt binds plan, effective configuration and runtime-lock digests.
  Recovery requires all three to match. A checkpoint never lowers verification.
- `propose_positions` validates a registered comparator. Proposals remain inactive
  until the same transaction commits a certificate and activates a snapshot.
  Monotonicity is checked again at commit; unknown comparators fail closed.
- Prepare a durable intent before dispatch. Once dispatch starts, interrupted or
  repeated dispatch requires read-back. `effect_recovery` distinguishes prepared,
  ambiguous and verified effects. It never invokes a provider or retries a write.
- A bundle supplies typed verified proofs for snapshot, disposition, coverage and
  cohort digests. Prepared effects must have matching durable verified receipts.
  Source and effect membership must be complete and disjoint across commit cohorts.
  Structural safety cannot be waived through exception declarations.
- Staged bundles are immutable. Exact commit replay returns the same certificate;
  changed content, outcome, cohort membership or proof requirements is rejected.
- Only `COMPLETED`, `COMPLETED_WITH_EXCEPTIONS` and `NO_CHANGE` can certify a commit.
  Gated, failed and abandoned outcomes never activate a snapshot or advance positions.
- Certificates form an inspectable digest-linked chain. Digests detect accidental
  inconsistency; they are not signatures or protection against a database owner.

## Boundaries and compatibility

The old `ratchet_runtime.Runner` remains available with its original file format
and callback contract. SQLite is an opt-in API, not a replacement `StateStore`.
Use a fresh database. This release does not import or migrate legacy or application
state, and does not promise wire compatibility with upstream application packages.
Python 3.9/3.10 users must remain on 0.5.x or upgrade Python before installing 0.6.

The caller verifies proof semantics and output bytes. A proof labelled verified
is not independently trustworthy merely because it has the right structure.
Ratchet checks identity and completeness, not the truth of an agent's judgment.
It records snapshot digests, not the snapshot files themselves; consumers must
persist immutable artifacts before committing and verify them during read-back.

`open_read_only` creates an in-memory snapshot, using a temporary copy when WAL
content exists. It leaves source files unchanged. Capture while writers are
quiescent: it detects observed file changes but is not a distributed or lock-free
consistent-copy protocol. An uncheckpointed WAL without its existing SHM is
rejected. Call `close()` when finished with the inspection snapshot.

Only local filesystem operation is claimed. Network filesystems, distributed
consensus, provider calls, target-side enforcement, scheduling and untrusted-code
isolation remain outside this API. A lease cannot cancel an external operation;
carry fencing tokens and idempotency keys to targets that support them.

## Lineage

The SQLite implementation and synthetic tests were extracted from the owner's
newer execution kernel. [Source hashes](UPSTREAM-PROVENANCE.json) record the exact
inputs. Application-specific proof special cases and knowledge contracts were
removed. Historical V2 requirement IDs in comments identify origin only; this
file is the public contract. See the changelog for public-only hardening.
