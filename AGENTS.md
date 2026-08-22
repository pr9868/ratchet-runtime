# Repository guidance

Ratchet Runtime is the public, release-oriented implementation of a generic execution-safety
contract for unattended agent workflows.

- Keep the repository organization-neutral and consumer-independent.
- Preserve deterministic ownership of ordering, tenure, positions, completion, effects, and runner
  state.
- Agent judgment may occur inside a step but may not weaken the runtime guarantees.
- Run `python3 -B tests/test_conformance.py` before committing.
- Keep `VERSION`, `pyproject.toml`, `setup.cfg`, `__version__`, contract, README, and changelog
  synchronized when behavior changes.
- Keep examples and documentation free of private paths, credentials, internal identifiers, and
  consumer-specific integration details.
