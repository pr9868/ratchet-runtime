# Local validation — 2026-09-29

Candidate: **0.6.0a1**. Local macOS arm64, Python 3.11.

| Check | Result |
|---|---|
| Legacy `python -B tests/test_conformance.py` | 63/63 passed |
| `python -B -m pytest -q tests/sqlite` | 37 passed |
| Wheel and source distribution build | Passed |
| Clean-environment wheel installation without runtime dependencies | Passed |
| Installed legacy effect-recovery example | Passed; exactly one synthetic target write |
| Installed SQLite commit example | Passed; atomic position/snapshot, exact replay, certificate chain and read-only inspection |
| Public code dependency/path scan | No private package imports, owner filesystem paths or credential patterns found |
| `git diff --check` | Passed |

The SQLite suite includes real competing processes and SIGKILL after intent,
dispatch, proposal, checkpoint, verified receipt, staging and commit. It checks
read-only source file invariance, stale fencing, recovery bindings, immutable
bundles, monotonic positions, required proof subjects, complete cohorts, certificate
chains, failed-outcome rejection and immutable replay policy.

The carried-forward legacy suite remains unchanged. Public-only hardening adds
failed/gated/abandoned outcome rejection, exact replay policy/outcome checks and
non-string canonical-key rejection. Source lineage is recorded separately.

Linux/Python 3.11, 3.12 and 3.13 CI is configured but has **not run on GitHub for
this candidate**. Real power-loss, live provider behavior, distributed storage and
production suitability are not established. Test counts describe local execution,
not publication or release approval.

`python scripts/check_release.py` checks version agreement, parses all public
Python files and compares the full repository file inventory to
`docs/SOURCE-MANIFEST.json`. After an intentional edit, review it and regenerate
with `--write-manifest` before revalidating or publishing.

## README expansion

The expanded landing page was checked for balanced code fences and valid local
file/section links. Its Python examples ran successfully against installed packages
in a temporary workspace. Anchor's documented legacy-helper init/validate/plan
sequence also passed in a fresh project. GitHub About metadata was checked for
field length and topic syntax. Mermaid diagrams still require GitHub rendering
review during publication. Runtime code and the recorded test suites are unchanged.
