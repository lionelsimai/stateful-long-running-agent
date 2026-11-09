"""Demo workflow: a simulated long task in six durable steps."""
from __future__ import annotations

import time
from typing import Any

from stateful_agent.engine import Step, Workflow

EXECUTION_LOG: list[str] = []


def _step(name: str, value: Any) -> Step:
    def fn(ctx: dict[str, Any]) -> dict[str, Any]:
        EXECUTION_LOG.append(name)
        time.sleep(0.01)
        return {name: value, f"{name}_done": True}

    deps: tuple[str, ...] = ()
    return Step(name=name, fn=fn, depends_on=deps)


def demo_workflow() -> Workflow:
    """Six-step simulated 24h-style pipeline (compressed for the demo)."""
    names = ["ingest", "clean", "chunk", "embed", "summarise", "publish"]
    steps: list[Step] = []
    for i, name in enumerate(names):
        deps = (names[i - 1],) if i else ()
        base = _step(name, f"result-{i + 1}")
        steps.append(Step(name=name, fn=base.fn, depends_on=deps))
    return Workflow(name="demo-long-task", steps=steps)
