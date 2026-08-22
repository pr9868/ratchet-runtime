# Review 001: The Green Suite Was Not the Contract

The first Ratchet design was written as four guarantees. An adversarial review found three of them
stronger than their mechanisms. Implementing the rewrite found more gaps, including gaps that a
green suite did not cover.

## Contract corrections

| Original claim | What was actually true | Correction |
|---|---|---|
| Completion was verified | The agent's `ok` result could become the marker | Mandatory runner-evaluated postconditions |
| One run wrote at a time | The lock behaved as a lease, but a displaced run could keep writing | Tenure revalidation, fencing, and guarded mutation |
| Failed windows could be retried | A crash after an external write could duplicate the effect | Intents, idempotency keys, and reconciliation |
| Runner state was safe | Agent and runner state had no explicit isolation boundary | Atomic state plus a documented process-isolation obligation |

That review expanded the contract to six guarantees. The implementation then corrected the contract
again: persisted heartbeats need a shared wall-clock representation, and a stale PID must not be
signaled because operating systems reuse process IDs.

## Public-release audit

The next source-level audit deliberately ignored the passing count and followed each guarantee to
the write that was supposed to enforce it.

| Finding | Why it mattered | Public alpha correction |
|---|---|---|
| Postconditions were optional | A step with only `ok: true` could still complete | Construction rejects missing postconditions |
| Stale breakers allocated tokens before ownership was serialized | Two breakers could return with the same fencing token | One `flock` critical section covers inspection, allocation, and publication |
| `TenureLost` called the normal finalizer | A displaced run still wrote breaker and run records | Tenure loss returns an in-memory outcome and persists nothing |
| `assert_held()` and mutation were separate | Seizure could occur between the check and write | `Tenure.mutate()` validates and writes under the coordinator guard |
| Heartbeat ran only between steps | One long step could look dead and be seized | Runner starts a background heartbeat thread |
| An orphan without reconciliation could fall through to invocation | An ambiguous external effect could be repeated blindly | Missing reconciliation blocks retry |
| `max_seconds` sounded like a hard kill | In-process callables were only checked between steps | Check after invocation and state the cooperative limit explicitly |

The controlled stale-breaker test now races eight processes against one expired tenure and requires
exactly one winner with exactly one new token. The tenure-loss test replaces ownership during a step
and requires that neither a breaker record nor a run record be written afterward.

## What 46/46 means

The current suite proves 46 assertions against the reference implementation on a local POSIX
filesystem. It includes real multiprocess acquisition and stale-seizure races. It does not prove
network-filesystem semantics, a real machine-power-loss boundary, live API idempotency, semantic
quality, or process isolation for agent code running as the same user.

The rule for future releases is narrower and more useful than “the suite is green”:

> Every guarantee must name its mechanism, its adversarial test, and the environment in which both
> are claimed.
