"""Checkpointed workflow engine: DAG, idempotency, resume.

Execution guarantee, stated plainly: **at-least-once execution with
idempotent replay**. A step whose checkpoint committed is never
re-executed. A step that was in flight when the process died *will*
run again on resume — with the *same* idempotency key, passed into
the step via its context, so a step that honours the key (as the demo
workflow does) applies its side effect exactly once. Steps that
ignore the key may apply their effect more than once; that is the
documented contract, not "effectively-once".
"""
from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from stateful_agent.store import Checkpoint, CheckpointStore

StepFn = Callable[[dict[str, Any]], dict[str, Any]]


def _canonical(payload: dict[str, Any]) -> str:
    """Canonical JSON of context *values* (sorted keys, stable types)."""
    return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))


def idempotency_key(run_id: str, step: str, payload: dict[str, Any]) -> str:
    """Stable idempotency key derived from the context values.

    The digest covers the canonicalised values the step will see —
    not just their key names — so two invocations with different
    inputs get different keys, and a replay after a crash (same
    values) gets the same key.
    """
    raw = f"{run_id}:{step}:{_canonical(payload)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


@dataclass(frozen=True)
class Step:
    """One workflow step; ``depends_on`` defines the DAG edges."""

    name: str
    fn: StepFn
    depends_on: tuple[str, ...] = ()


@dataclass
class Workflow:
    """A DAG of steps. List order is irrelevant; dependencies rule."""

    name: str
    steps: list[Step]

    def validate(self) -> None:
        """Raise ValueError on duplicates, missing deps or cycles."""
        names = [s.name for s in self.steps]
        if len(set(names)) != len(names):
            raise ValueError("duplicate step name")
        known = set(names)
        for step in self.steps:
            missing = set(step.depends_on) - known
            if missing:
                raise ValueError(f"step {step.name} depends on unmet {missing}")
        topological_order(self)  # raises on cycles


def topological_order(workflow: Workflow) -> list[Step]:
    """Return steps in dependency order (Kahn's algorithm).

    Deterministic: among ready steps, workflow list order wins.
    Raises ValueError if the graph has a cycle.
    """
    by_name = {s.name: s for s in workflow.steps}
    done: list[Step] = []
    placed: set[str] = set()
    remaining = list(workflow.steps)
    while remaining:
        ready = [s for s in remaining if set(s.depends_on) <= placed]
        if not ready:
            raise ValueError(f"dependency cycle among {[s.name for s in remaining]}")
        for step in ready:
            done.append(by_name[step.name])
            placed.add(step.name)
            remaining.remove(step)
    return done


@dataclass
class RunResult:
    """Outcome of a run/resume."""

    run_id: str
    status: str
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    executed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    reconciled: bool = False


class WorkflowEngine:
    """Runs workflows, checkpointing around every step so a crash is resumable."""

    def __init__(self, store: CheckpointStore) -> None:
        """Bind the engine to a checkpoint store."""
        self.store = store

    def run(self, workflow: Workflow, run_id: str,
            context: dict[str, Any] | None = None,
            on_step_completed: Callable[[str], None] | None = None) -> RunResult:
        """Run or resume ``workflow`` under ``run_id``.

        - Completed steps (checkpoint committed) are skipped.
        - A stale ``running`` status left by a dead process is
          reconciled to ``interrupted`` before resuming.
        - The per-run lock is held for the whole run; a second live
          process running the same run id gets ``RunLockedError``.
        - Each step sees ``ctx["_idempotency_key"]`` and
          ``ctx["_step_name"]``; its start is recorded *before* it runs.
        """
        workflow.validate()
        order = topological_order(workflow)
        ctx: dict[str, Any] = dict(context or {})
        prior_status = self.store.status(run_id)
        self.store.create_run(run_id, workflow.name)
        self.store.acquire_run_lock(run_id)
        # We hold the lock, so a prior "running" status can only be a
        # stale claim from a process that died mid-run: reconcile it.
        reconciled = prior_status == "running"
        try:
            if reconciled:
                self.store.set_status(run_id, "interrupted")
            self.store.set_status(run_id, "running")
            done = self.store.completed_steps(run_id)
            result = RunResult(run_id=run_id, status="running",
                               outputs=dict(done), reconciled=reconciled)
            try:
                for step in order:
                    if step.name in done:
                        result.skipped.append(step.name)
                        ctx.update(done[step.name])
                        continue
                    key = idempotency_key(run_id, step.name, dict(ctx))
                    self.store.record_step_start(run_id, step.name, key)
                    ctx["_idempotency_key"] = key
                    ctx["_step_name"] = step.name
                    out = step.fn(ctx)
                    if not isinstance(out, dict):
                        raise TypeError(
                            f"step {step.name} must return dict, got {type(out)}")
                    ctx.pop("_idempotency_key", None)
                    ctx.pop("_step_name", None)
                    self.store.save(Checkpoint(run_id, step.name, key, out, time.time()))
                    ctx.update(out)
                    result.outputs[step.name] = out
                    result.executed.append(step.name)
                    if on_step_completed is not None:
                        on_step_completed(step.name)
                self.store.set_status(run_id, "completed")
                result.status = "completed"
            except Exception:
                ctx.pop("_idempotency_key", None)
                ctx.pop("_step_name", None)
                self.store.set_status(run_id, "interrupted")
                result.status = "interrupted"
                raise
            return result
        finally:
            self.store.release_run_lock(run_id)
