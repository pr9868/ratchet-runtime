"""Executable crash-after-effect recovery example for Ratchet Runtime.

The JSON file stands in for an external API that accepts an idempotency key and a
fencing token. The first run lands the effect and then raises before returning a
result. The second run reads the orphan intent, observes the target, passes the
normal postcondition, and completes without invoking the effect again.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Optional

from ratchet_runtime import (
    EffectState,
    Reconciliation,
    Runner,
    StateStore,
    Step,
)


class FileTarget:
    """Tiny target-system stand-in with idempotency and fencing enforcement."""

    def __init__(self, path: Path):
        self.path = path

    def read(self) -> Optional[dict]:
        return json.loads(self.path.read_text()) if self.path.exists() else None

    def put(self, *, key: str, fence: int, value: str) -> None:
        current = self.read()
        if current and fence < current["fence"]:
            raise RuntimeError("stale fencing token")
        if current and key == current["key"]:
            return  # the same logical effect is already present
        writes = int(current["writes"]) + 1 if current else 1
        self.path.write_text(json.dumps({
            "key": key,
            "fence": fence,
            "value": value,
            "writes": writes,
        }))


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ratchet-example-") as root:
        root_path = Path(root)
        target = FileTarget(root_path / "external-target.json")
        store = StateStore(str(root_path / "runner-state"))

        effect_key = lambda _ctx: "publish:quarterly-report:2026-q2"  # noqa: E731

        def observed(ctx, _result):
            record = target.read()
            return bool(
                record
                and record["key"] == ctx.idempotency_key
                and record["value"] == "quarterly report"
            )

        def publish_then_crash(ctx):
            target.put(
                key=ctx.idempotency_key,
                fence=ctx.tenure_token,
                value="quarterly report",
            )
            raise RuntimeError("simulated crash after target write")

        first = Runner(store, [Step(
            "publish",
            invoke=publish_then_crash,
            postcondition=observed,
            side_effecting=True,
            idempotency_key=effect_key,
        )]).run()

        def reconcile(_ctx, intent):
            record = target.read()
            if record is None:
                return Reconciliation(EffectState.ABSENT)
            if record["key"] == intent["idempotency_key"]:
                return Reconciliation(
                    EffectState.PRESENT,
                    result={"positions": {"source-a": "cursor-42"}},
                )
            return Reconciliation(EffectState.UNKNOWN)

        second = Runner(store, [Step(
            "publish",
            invoke=lambda _ctx: (_ for _ in ()).throw(
                AssertionError("reconciliation should prevent a duplicate invocation")
            ),
            postcondition=observed,
            side_effecting=True,
            idempotency_key=effect_key,
            reconcile=reconcile,
        )]).run()

        record = target.read()
        assert not first.completed
        assert second.completed and second.reconciled == ["publish"]
        assert record and record["writes"] == 1
        assert store.positions() == {"source-a": "cursor-42"}

        print(f"first run:  completed={first.completed} reason={first.reason}")
        print(f"second run: completed={second.completed} reconciled={second.reconciled}")
        print(f"target writes: {record['writes']} (expected 1)")


if __name__ == "__main__":
    main()
