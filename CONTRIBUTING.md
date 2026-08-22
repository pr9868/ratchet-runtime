# Contributing

Ratchet Runtime is intentionally small. Changes should strengthen or clarify the execution-safety
contract without adding domain or agent-framework behavior.

Before opening a change:

```bash
python3 -B tests/test_conformance.py
python3 -m build
```

Behavior changes must update the implementation, contract, tests, README status, and changelog
together. A weakened guarantee is a contract change, not an implementation detail.

Please keep examples organization-neutral and free of credentials, private identifiers, internal
paths, and consumer-specific workflow logic.
