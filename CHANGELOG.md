# Changelog

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
