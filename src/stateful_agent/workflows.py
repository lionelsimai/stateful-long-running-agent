"""Demo workflow: a simulated long task in six durable steps.

Steps demonstrate the idempotency contract: each step receives its
idempotency key in ``ctx["_idempotency_key"]`` and records its side
effect (an append to the effect log) **only if that key has not
already produced an effect**. A step re-run after a crash therefore
replays without duplicating its effect.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from stateful_agent.engine import Step, Workflow

EXECUTION_LOG: list[str] = []
_SEEN_KEYS: set[str] = set()
STEP_NAMES = ["ingest", "clean", "chunk", "embed", "summarise", "publish"]


def _record_effect(name: str, key: str, effects_path: Path | None) -> None:
    """Apply the step's side effect exactly once per idempotency key."""
    if effects_path is not None:
        keys_file = effects_path.with_suffix(".keys")
        seen = set(keys_file.read_text().split()) if keys_file.exists() else set()
        if key in seen:
            return
        with effects_path.open("a") as fh:
            fh.write(name + "\n")
        with keys_file.open("a") as fh:
            fh.write(key + "\n")
        return
    if key in _SEEN_KEYS:
        return
    _SEEN_KEYS.add(key)
    EXECUTION_LOG.append(name)


def _make_step(name: str, value: Any, deps: tuple[str, ...],
               effects_path: Path | None, delay: float) -> Step:
    def fn(ctx: dict[str, Any]) -> dict[str, Any]:
        key = str(ctx.get("_idempotency_key", f"nokey-{name}"))
        _record_effect(name, key, effects_path)
        if delay:
            time.sleep(delay)
        return {name: value, f"{name}_done": True}

    return Step(name=name, fn=fn, depends_on=deps)


def demo_workflow(effects_path: Path | None = None) -> Workflow:
    """Six-step simulated 24h-style pipeline (compressed for the demo).

    With ``effects_path`` set, step side effects are appended to that
    file (once per idempotency key, tracked in a sibling ``.keys``
    file) so crash tests can count them across processes.
    """
    steps: list[Step] = []
    for i, name in enumerate(STEP_NAMES):
        deps = (STEP_NAMES[i - 1],) if i else ()
        steps.append(_make_step(name, f"result-{i + 1}", deps, effects_path, delay=0.01))
    return Workflow(name="demo-long-task", steps=steps)


def crash_workflow(effects_path: Path) -> Workflow:
    """Same six steps as the demo, slower, with on-disk effects.

    Used by the kill -9 worker and the resume that follows it, so both
    processes share one workflow definition and one effect log.
    """
    steps: list[Step] = []
    for i, name in enumerate(STEP_NAMES):
        deps = (STEP_NAMES[i - 1],) if i else ()
        steps.append(_make_step(name, f"result-{i + 1}", deps, effects_path, delay=0.05))
    return Workflow(name="demo-long-task", steps=steps)
