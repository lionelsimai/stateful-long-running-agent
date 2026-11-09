"""Checkpointed workflow engine: DAG, idempotency, resume."""
from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from stateful_agent.store import Checkpoint, CheckpointStore

StepFn = Callable[[dict[str, Any]], dict[str, Any]]


def idempotency_key(run_id: str, step: str, payload: dict[str, Any]) -> str:
    """Stable idempotency key for a step invocation."""
    raw = f"{run_id}:{step}:{sorted(payload.items())}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


@dataclass(frozen=True)
class Step:
    """One workflow step; ``depends_on`` defines the DAG edges."""

    name: str
    fn: StepFn
    depends_on: tuple[str, ...] = ()


@dataclass
class Workflow:
    """An ordered DAG of steps (list order must respect dependencies)."""

    name: str
    steps: list[Step]

    def validate(self) -> None:
        """Raise ValueError if the DAG is invalid."""
        seen: set[str] = set()
        for step in self.steps:
            if step.name in seen:
                raise ValueError(f"duplicate step: {step.name}")
            missing = set(step.depends_on) - seen
            if missing:
                raise ValueError(f"step {step.name} depends on unmet {missing}")
            seen.add(step.name)


@dataclass
class RunResult:
    """Outcome of a run/resume."""

    run_id: str
    status: str
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    executed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


class WorkflowEngine:
    """Runs workflows, checkpointing after every step so a crash is resumable."""

    def __init__(self, store: CheckpointStore) -> None:
        """Bind the engine to a checkpoint store."""
        self.store = store

    def run(self, workflow: Workflow, run_id: str,
            context: dict[str, Any] | None = None) -> RunResult:
        """Run or resume ``workflow`` under ``run_id``. Completed steps are skipped."""
        workflow.validate()
        ctx: dict[str, Any] = dict(context or {})
        self.store.create_run(run_id, workflow.name)
        done = self.store.completed_steps(run_id)
        result = RunResult(run_id=run_id, status="running", outputs=dict(done))
        try:
            for step in workflow.steps:
                if step.name in done:
                    result.skipped.append(step.name)
                    ctx.update(done[step.name])
                    continue
                payload = {"context_keys": sorted(ctx)}
                out = step.fn(ctx)
                if not isinstance(out, dict):
                    raise TypeError(f"step {step.name} must return dict, got {type(out)}")
                key = idempotency_key(run_id, step.name, payload)
                self.store.save(Checkpoint(run_id, step.name, key, out, time.time()))
                ctx.update(out)
                result.outputs[step.name] = out
                result.executed.append(step.name)
            self.store.set_status(run_id, "completed")
            result.status = "completed"
        except Exception:
            self.store.set_status(run_id, "interrupted")
            result.status = "interrupted"
            raise
        return result
