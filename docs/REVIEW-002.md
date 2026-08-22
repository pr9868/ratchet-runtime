# Review 002: Public-alpha release-candidate audit

**Date:** 2026-08-21

**Scope:** runtime, contract, architecture, packaging, examples, and conformance suite

**Method:** trace each claimed guarantee to its final write, then attack the gaps between callbacks,
recovery decisions, and state transitions

## Verdict

**Ready for a local POSIX public alpha after corrections. Not approved as a production engine or a
distributed coordinator.**

The original 46 checks continued to pass after the corrections. Seventeen adversarial regression
checks were added, producing a 63/63 suite. The release remains gated on the exclusions below rather
than treating the passing count as broader proof.

## Findings and corrections

| Severity | Finding | Failure mode | Correction and evidence |
|---|---|---|---|
| Critical | Reconciled effects skipped the step postcondition | A boolean `True` from recovery could establish completion without the normal read-back | `PRESENT` now passes the recovered result through the same postcondition; a failing check retains the intent |
| Critical | Boolean reconciliation merged `ABSENT` with `UNKNOWN` | An uncertain read could be interpreted as permission to retry and duplicate an effect | `Reconciliation` requires tri-state `PRESENT`, `ABSENT`, or `UNKNOWN`; only `ABSENT` reaches retry |
| Critical | A side-effecting pause cleared its intent | The next run could invoke the effect again because neither completion nor recovery evidence remained | Side-effecting pause now fails closed and retains the intent |
| Critical | A successful side effect cleared its intent before workflow completion | A later-step failure or crash could erase recovery evidence and duplicate the earlier effect on restart | Resolved intents now remain until the completion record is durable; completed-run cleanup residue is self-healing |
| High | A changed key could overwrite an orphan intent | A workflow revision could retry the same step as a different logical operation | The newly evaluated key must exactly match the orphaned key before an `ABSENT` retry |
| High | Step names were used as filenames without validation | `../` or `/` could escape the intent directory | Step and dependency names are restricted to safe identifiers |
| High | The key function was checked only for presence | `None`, an empty string, or an exception could reach invocation without usable idempotency | The runtime evaluates and validates a non-empty string before writing intent or invoking the step |
| High | Setup after acquisition sat outside cleanup | Corrupt completion state could raise after heartbeat start and strand live tenure | All post-acquisition work is now inside the release `finally` boundary |
| High | Unexpected heartbeat exceptions died silently | A runner could continue after losing its ability to refresh ownership | Any heartbeat I/O or state exception becomes `TenureLost`; the run then persists no outcome writes |
| Medium | `RunContext` exposed `StateStore` directly | Trusted callback examples encouraged accidental writes across the runner boundary | The callback context now exposes step name, effect key, fencing token, positions, and scratch only |
| Medium | Result and postcondition truthiness were permissive | `"yes"`, malformed positions, strings in `degraded`, or NaN could enter the state path | The runner validates boolean outcomes, finite JSON metadata, pause reasons, and real boolean postconditions |
| Medium | Structurally invalid lock JSON was not classified as corruption | A valid JSON object with missing fields could brick acquisition instead of following the documented corrupt-lock path | Lock records now receive schema and finite-value checks before use |
| Medium | Human breaker reset was not coordinated | A reset could overwrite breaker state while a run still held tenure | Reset uses the coordinator and refuses while a live owner exists |
| Low | Invalid timing and breaker configuration was accepted | Zero-period heartbeats could spin and nonpositive limits produced surprising behavior | Lease and budget values are validated at construction |

## Evidence added

The added checks cover path traversal, callback context boundaries, cleanup after acquired-tenure
failure, fail-closed heartbeat errors, reset races, strict postconditions, malformed result schemas,
postconditions after reconciliation, empty and drifting effect keys, `UNKNOWN` recovery, same-key
`ABSENT` retry, side-effecting pause, later-step failure after an effect, completed-run cleanup
residue, structural lock corruption, and invalid configuration.

The executable [`effect_recovery.py`](../examples/effect_recovery.py) scenario also exercises the
human-readable path: intent, external write, simulated crash, orphan read-back, normal postcondition,
reconciled completion, and exactly one target write.

## Residual risks and non-claims

- `flock` is a local POSIX choice. NFS, SMB, Windows, and multi-host coordination are untested and
  outside the contract.
- Filesystem durability has not been tested by cutting machine power. `fsync` calls express the
  intended boundary but do not certify a particular disk/controller stack.
- Completion is one atomic file containing marker, positions, and outcome. The breaker and per-run
  history are separate durable files, not a cross-file transaction.
- The runtime carries an idempotency key and fencing token to callbacks; only the target system can
  enforce them.
- Python mode bits and omission of `StateStore` from `RunContext` do not isolate malicious code
  running as the same OS identity.
- Time budgets remain cooperative around in-process callables. A hard deadline requires process
  isolation and termination outside this module.
- The suite establishes mechanical execution behavior, not the semantic quality of an agent's work
  or a live API's idempotency behavior.

## Release checks

```bash
python3 -B tests/test_conformance.py
python3 -m build
python3 examples/effect_recovery.py
```

The acceptance rule remains: each guarantee names a mechanism, an adversarial check, and the
environment in which the claim holds.
