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
| **G5 Effect integrity** | Side effects have idempotency keys, intent records, and reconciliation before retry |
| **G6 State integrity** | Completion, positions, intents, outcomes, and tenure are durable and atomically published |

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

workspace = Path("work")
workspace.mkdir(exist_ok=True)

def write_report(_ctx):
    (workspace / "report.md").write_text("verified output\n")
    return {"ok": True, "positions": {"source-a": "cursor-42"}}

steps = [
    Step(
        "write-report",
        invoke=write_report,
        postcondition=lambda _ctx, _result: (workspace / "report.md").exists(),
    )
]

outcome = Runner(StateStore(".ratchet-state"), steps).run()
assert outcome.completed
```

For an external write, add `side_effecting=True`, an `idempotency_key`, and a `reconcile` callback
that can read the target system after an interrupted run. See the effect lifecycle in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Why the guard exists

An earlier implementation used an atomic rename for stale-lock seizure but allocated its fencing
token before ownership was serialized. Two breakers could both return with the same token. The
public runtime uses a small `flock`-protected coordinator so inspecting the lock, allocating the next
token, and publishing ownership happen in one local-filesystem critical section. The same guard
wraps every runner-state mutation.

The conformance suite also verifies that losing tenure produces no breaker or run-record write,
that a background heartbeat stays live during a long step, and that a missing postcondition or
idempotency key is rejected at construction.

## Status and limits

**v0.5.0a1: public alpha, 46/46 conformance checks passing.**

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
3. [`docs/REVIEW-001.md`](docs/REVIEW-001.md) — what the first design and first green suite got wrong.
4. [`tests/test_conformance.py`](tests/test_conformance.py) — executable evidence for the claims.

Ratchet Runtime is licensed under the [MIT License](LICENSE).
