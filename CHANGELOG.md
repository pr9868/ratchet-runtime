# Changelog

## 0.6.0a1 — 2026-09-29 (local candidate)

- Added the domain-neutral `ratchet_sqlite` API: transactional leases and fencing,
  immutable attempts, monotonic positions, checkpoints, effect recovery, atomic
  snapshot activation, and chained commit certificates.
- Added inspection that does not write to the source database or its sidecars.
- Extracted generic typed contracts; no Tracker runtime, provider, or knowledge dependency.
- Preserved the legacy Runner/StateStore API and its state format.
- Reject failed/gated/abandoned commits and outcome or verification-policy drift on replay.
- Canonical serialization rejects non-string mapping keys instead of silently colliding them.
- Carried forward synthetic component, SIGKILL recovery, and multiprocess tests; added
  public-boundary regressions and installation smoke checks.
- **Compatibility:** the distribution now requires Python 3.11+. SQLite is a separate API
  and store; there is no automatic migration from file-backed or other application state.

## 0.5.0a2 — 2026-08-21

Release-candidate adversarial corrections.

- Replaced boolean recovery with explicit `PRESENT`, `ABSENT`, and `UNKNOWN` reconciliation.
- Required recovered effects to pass the normal runner postcondition.
- Blocked key drift across orphan recovery and validated effect keys before invocation.
- Retained intents when a side-effecting step attempts to pause.
- Retained resolved intents until workflow completion so later failures cannot erase recovery
  evidence, with self-healing cleanup residue after a durable commit.
- Restricted step names to safe identifiers and removed `StateStore` from callback context.
- Exposed the evaluated idempotency key and fencing token to callbacks.
- Moved all post-acquisition setup under cleanup and made heartbeat errors fail closed.
- Added strict result schemas, structural lock validation, coordinated breaker reset, and early
  configuration validation.
- Added the executable crash-after-effect example, a local-coordinator ADR, Review 002, and seventeen
  adversarial checks; 63/63 checks pass.

## 0.5.0a1 — 2026-08-21

First public alpha.

- Published the six-guarantee execution-safety contract and architecture boundary.
- Serialized acquisition, stale seizure, fencing-token allocation, and state mutation with a POSIX
  `flock` coordinator.
- Added a background heartbeat for Runner-managed tenure.
- Made runner-evaluated postconditions mandatory.
- Made idempotency keys mandatory for side-effecting steps.
- Stopped blind retry when an orphan intent has no reconciliation function.
- Stopped all persistent Runner writes after tenure loss.
- Added concurrent stale-breaker, lost-tenure, long-step-heartbeat, declaration, orphan, and budget
  conformance checks; 46/46 checks pass.
- Documented local-filesystem, process-isolation, watchdog, external-effect, and cooperative-timeout
  limits.
