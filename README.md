# Ratchet Runtime

**Execution safety for unattended agent workflows.**

An agent can choose how to perform a step. It should not decide whether that step ran, whether an
external effect is safe to repeat, or whether a run deserves a completion marker. Ratchet Runtime
makes those decisions in deterministic Python.

It is a small contract and reference implementation, not a scheduler, agent framework, or semantic
judge. It owns the mechanics that must remain true when an agent is confidently wrong.

## Guarantees

| Guarantee | Runtime responsibility |
|---|---|
| **G1 Ordering** | A step cannot run before its declared predecessors complete |
| **G2 Exclusive runner tenure** | One run owns runner state; fencing tokens increase on every acquisition |
| **G3 Position integrity** | Source positions advance only with verified completion |
| **G4 Verified completion** | Every step has a runner-evaluated postcondition; `ok: true` is not sufficient |
| **G5 Effect integrity** | Side effects have validated idempotency keys, intent records, and tri-state reconciliation before retry |
| **G6 State integrity** | State files are durably replaced; completion, positions, and outcome share one atomic record |

The implementation also provides cooperative run budgets, a circuit breaker, deliberate pause
semantics, and a staleness check intended to run from a separate watchdog process.

## Quick start

Ratchet Runtime has no third-party runtime dependencies. It currently targets Python 3.9+ on Linux
and macOS because tenure serialization uses POSIX `flock`.

```bash
git clone https://github.com/pr9868/ratchet-runtime.git
cd ratchet-runtime
python3 -m pip install -e .
python3 -B tests/test_conformance.py
```

A step must declare a mechanical postcondition. Side-effecting steps must also declare a stable
idempotency key.

```python
from pathlib import Path
from ratchet_runtime import Runner, StateStore, Step

source = Path("source.txt")
source.write_text("alpha\nbeta\n")

def count_lines(_ctx):
    return {"ok": True, "line_count": len(source.read_text().splitlines())}

steps = [Step(
    "count-lines",
    invoke=count_lines,
    # Re-read the source instead of accepting the step's count as truth.
    postcondition=lambda _ctx, result: (
        result["line_count"] == len(source.read_text().splitlines())
    ),
)]

outcome = Runner(StateStore(".ratchet-state"), steps).run()
assert outcome.completed
```

Use one state root per workflow. Step names are safe identifiers rather than paths. `RunContext`
exposes the current `tenure_token` and the runtime-evaluated `idempotency_key`; it deliberately does
not expose the state store.

For an external write, add `side_effecting=True`, a stable `idempotency_key`, and a tri-state
`reconcile` callback that can return `PRESENT`, `ABSENT`, or `UNKNOWN` after an interrupted run. The
complete example simulates a crash after a target write and proves the next run does not duplicate
it:

```bash
python3 examples/effect_recovery.py
```

See [`examples/effect_recovery.py`](examples/effect_recovery.py) and the effect lifecycle in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Why the guard exists

An earlier implementation used an atomic rename for stale-lock seizure but allocated its fencing
token before ownership was serialized. Two breakers could both return with the same token. The
public runtime uses a small `flock`-protected coordinator so inspecting the lock, allocating the next
token, and publishing ownership happen in one local-filesystem critical section. The same guard
wraps every runner-state mutation.

The conformance suite also verifies that losing tenure produces no breaker or run-record write,
that a background heartbeat stays live during a long step, and that missing postconditions, empty
effect keys, key drift, ambiguous reconciliation, malformed results, and unsafe side-effect pauses
fail closed.

## Status and limits

**v0.5.0a2: public alpha, 63/63 conformance checks passing.**

The checks establish the contract on a local POSIX filesystem. They do not establish:

- safe locking on NFS, SMB, or another network filesystem;
- machine-crash durability under a real power loss;
- idempotency against a live external API;
- semantic quality of an agent's judgment;
- a hard kill of an in-process callable when `max_seconds` expires;
- state isolation when agent code runs as the same OS identity and can address the state path; or
- where the required out-of-process staleness watchdog is scheduled.

Use a separate process identity or a mediated runner API when agent steps are not trusted with the
runner's filesystem. Carry the tenure token to external systems that support fencing. The runtime
cannot manufacture either protection inside a Python callback.

## Read next

1. [`docs/CONTRACT.md`](docs/CONTRACT.md) — the exact guarantees and deployment obligations.
2. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — the state boundary and effect lifecycle.
3. [`docs/ADR-001.md`](docs/ADR-001.md) — why the reference coordinator is local POSIX `flock`.
4. [`docs/REVIEW-001.md`](docs/REVIEW-001.md) — what the first design and first green suite got wrong.
5. [`docs/REVIEW-002.md`](docs/REVIEW-002.md) — the release-candidate adversarial review.
6. [`tests/test_conformance.py`](tests/test_conformance.py) — executable evidence for the claims.

Ratchet Runtime is licensed under the [MIT License](LICENSE).
