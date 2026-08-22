# The Ratchet Runtime Contract

**Version:** 0.5.0-alpha.2
**Status:** public alpha and executable reference contract

## Scope

Ratchet Runtime covers unattended execution of an ordered workflow in which one or more steps may
be performed by an agent and one or more steps may affect an external system.

It does not decide what a step should do, whether an agent's judgment is wise, when a schedule
fires, or how an output is rendered. It constrains sequence, ownership, effects, and completion.

The reference implementation is conformant only on a local POSIX filesystem. A deployment must add
process isolation for untrusted agent code and target-side fencing where an external system supports
it.

## G1 — Ordering

- Step order is declared before the run.
- Step names are safe identifiers, not filesystem paths.
- A step cannot run before each declared predecessor has completed successfully.
- `ok: false`, an invalid result, a failed postcondition, or an unmet dependency blocks successors.
- A failed or incomplete run writes no new completion assertion.

## G2 — Exclusive runner tenure

Each acquisition publishes `run_id`, `pid`, `host`, `started_at`, `heartbeat_at`, and a strictly
increasing `tenure` token.

- Inspection, stale seizure, token allocation, and publication are serialized by a POSIX `flock`
  coordinator.
- The complete lock record is atomically published; no reader sees a partially written record.
- Runner-managed tenure heartbeats from a background thread while a step is executing.
- TTL derives from heartbeat period, not historical run duration.
- A stale or corrupt lock may be seized, but the recorded PID is never signaled: PIDs can be reused.
- Every runner-state mutation revalidates ownership while the same coordinator guard excludes
  seizure.
- A run that has lost tenure performs no further persistent runner-state writes.
- Release is ownership-checked.
- The current fencing token is available to the step through `RunContext.tenure_token`.
- External systems should reject lower tenure tokens where they support fencing. Where they do not,
  G5's intent, idempotency, and reconciliation rules are the protection available.

The local coordinator is deliberately narrower than a distributed lock. NFS, SMB, multi-host
coordination, and a target that ignores fencing are outside the reference implementation's claim.

## G3 — Position integrity

- Positions are per source, not per workflow.
- A position represents coverage actually read, preferably an opaque source cursor.
- Positions remain pending during a run.
- Completion, positions, and the run outcome are committed in one completion record.
- A failed or paused run advances no position.
- A degraded source retains its previous position while independently successful sources may
  advance.
- A consumer using timestamp positions must declare a safety overlap and deduplicate by stable ID.
- The runtime does not compare opaque cursor values or infer that one cursor is newer than another.
  Rewinds and source-specific monotonicity checks belong in the workflow's postconditions.

## G4 — Verified completion

- Every step declares a deterministic postcondition evaluated by the runner.
- A missing postcondition is rejected when the step is constructed.
- `ok: true` is necessary and never sufficient.
- A postcondition that returns false or raises fails the step.
- A postcondition must return a real boolean; truthy strings or objects are ambiguity and fail.
- A recovered `PRESENT` effect runs the same postcondition before the step is accepted.
- Completion is asserted only after every postcondition holds.
- A deliberate pause must itself pass a postcondition, then releases tenure without completion or
  position advancement.
- A side-effecting step cannot pause after invocation. It fails and retains its intent so the next
  run must reconcile the effect.

Postconditions establish mechanical facts such as an artifact existing, a target read-back matching,
or a count changing. They do not establish semantic quality.

## G5 — Effect integrity

Ratchet Runtime provides an explicit restart protocol, not exactly-once execution.

- Every side-effecting step declares a key function that must return a non-empty string before
  invocation. The evaluated key is available as `RunContext.idempotency_key`.
- The runner writes an intent record before invoking the step.
- A passed or reconciled side effect retains its intent until the workflow completion record is
  durable. A later failure therefore cannot erase recovery evidence for an earlier effect.
- Intent cleanup happens after workflow completion. Residue whose `run_id` matches the authoritative
  completion record is safe to remove at the start of the next run.
- An orphan intent means the previous attempt may have landed its effect.
- An orphan is reconciled against the external system as `PRESENT`, `ABSENT`, or `UNKNOWN`.
- `PRESENT` runs the normal postcondition and skips invocation; `ABSENT` may retry; `UNKNOWN` stops
  for human review.
- An `ABSENT` retry is allowed only when the newly evaluated key exactly matches the orphaned key.
- If no reconciliation function exists, the step stops instead of retrying blindly.
- A non-idempotent operation is marked `at_most_once` and requires human reconciliation after an
  ambiguous interruption.

## G6 — State integrity and isolation boundary

- Persistent JSON is written by temp file, file fsync, atomic replace, and directory fsync.
- Corrupt historical state is an error, never silently treated as absent.
- Completion contains marker, positions, and outcome in one atomic file.
- Runner state is created with mode `0700` and mutation is mediated through tenure.
- A `RunContext` handed to a callback does not expose the `StateStore`.
- Each workflow uses a separate state root; intent names are scoped to that workflow.

Mode bits do not isolate code running as the same OS identity. A deployment that treats the agent as
untrusted must run it under a separate identity or behind a mediated API that cannot address the
runner-state path. That is a deployment obligation, not a property Python can infer.

## Boundedness and observability

- `max_steps` prevents a run from beginning more than the declared number of steps.
- `max_seconds` is checked before and after each in-process step. An over-budget step cannot commit
  completion. Reconciliation and postcondition evaluation are also checked after returning, but the
  reference runtime does not forcibly terminate an in-process callable.
- Consecutive failures open a circuit breaker that requires explicit reset.
- Every owned success, failure, or pause writes a run record. A run that loses tenure returns only an
  in-memory outcome because writing its diagnosis would violate G2.
- `staleness_check` must be scheduled outside the Runner. Code inside a run cannot detect that no run
  started.

## Conformance

The executable suite must test every claimed mechanism without invoking an LLM:

```bash
python3 -B tests/test_conformance.py
```

A green suite establishes the tested mechanisms under its stated environment. It does not convert
the exclusions above into guarantees. Review findings that changed this wording and implementation
are recorded in [`REVIEW-001.md`](REVIEW-001.md) and [`REVIEW-002.md`](REVIEW-002.md).
