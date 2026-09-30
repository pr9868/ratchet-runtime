# Contributing

Ratchet Runtime is intentionally small. Changes should strengthen or clarify the execution-safety
contract without adding domain or agent-framework behavior.

Before opening a change:

```bash
python3 -m pip install ".[test]" build
python3 -B tests/test_conformance.py
python3 -B -m pytest -q tests/sqlite
python3 -m build
python3 examples/effect_recovery.py
python3 examples/sqlite_commit.py
```

The `build` command requires the standard PyPA `build` frontend. If it is not installed, verify the
same local setuptools package path with `python3 -m pip wheel . --no-deps --no-build-isolation`.

Behavior changes must update the implementation, contract, tests, README status, and changelog
together. A weakened guarantee is a contract change, not an implementation detail.

Please keep examples organization-neutral and free of credentials, private identifiers, internal
paths, and consumer-specific workflow logic.
