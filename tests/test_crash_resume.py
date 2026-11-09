"""Deterministic kill -9 crash test (real subprocess, real SIGKILL).

The worker blocks after its 2nd checkpoint commits and writes a
ready flag; the test then SIGKILLs it. Resume must skip exactly the
2 committed steps and the side-effect log must end with each of the
6 steps exactly once.
"""
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from stateful_agent.engine import Step, Workflow, WorkflowEngine, idempotency_key
from stateful_agent.errors import CheckpointConflictError, RunLockedError
from stateful_agent.store import Checkpoint, CheckpointStore
from stateful_agent.workflows import STEP_NAMES, crash_workflow

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "examples" / "_slow_worker.py"


def test_kill9_resume_skips_committed_and_effects_exactly_once(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db, effects, flag = tmp_path / "c.db", tmp_path / "effects.log", tmp_path / "ready.flag"
    proc = subprocess.Popen(
        [sys.executable, str(WORKER), str(db), str(effects), str(flag)], cwd=str(ROOT))
    try:
        deadline = time.time() + 30
        while not flag.exists():
            assert time.time() < deadline, "worker never signalled readiness"
            assert proc.poll() is None, "worker exited before blocking"
            time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == -signal.SIGKILL

    store = CheckpointStore(db)
    assert list(store.completed_steps("crash-run")) == STEP_NAMES[:2]
    assert store.status("crash-run") == "running"  # stale, left by the kill
    result = WorkflowEngine(store).run(crash_workflow(effects), "crash-run")
    assert result.status == "completed"
    assert result.reconciled is True  # stale "running" was reconciled
    assert result.skipped == STEP_NAMES[:2]
    assert result.executed == STEP_NAMES[2:]
    lines = effects.read_text().split()
    assert len(lines) == 6 and sorted(lines) == sorted(STEP_NAMES)


def test_topological_execution_ignores_list_order(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = CheckpointStore(tmp_path / "t.db")
    calls: list[str] = []

    def mk(name: str):  # type: ignore[no-untyped-def]
        return lambda ctx: calls.append(name) or {name: 1}

    wf = Workflow("w", [Step("c", mk("c"), ("b",)), Step("a", mk("a")), Step("b", mk("b"), ("a",))])
    result = WorkflowEngine(store).run(wf, "topo1")
    assert result.executed == ["a", "b", "c"] and calls == ["a", "b", "c"]


def test_cycle_rejected() -> None:
    wf = Workflow("w", [Step("a", lambda c: {}, ("b",)), Step("b", lambda c: {}, ("a",))])
    with pytest.raises(ValueError):
        wf.validate()


def test_idempotency_key_depends_on_values_not_names() -> None:
    base = idempotency_key("r", "s", {"a": 1, "b": "x"})
    assert base == idempotency_key("r", "s", {"b": "x", "a": 1})  # order-free
    assert base != idempotency_key("r", "s", {"a": 1, "b": "y"})  # value changes key
    assert base != idempotency_key("r", "s", {"a": 1})  # missing value changes key


def test_step_start_recorded_before_failure(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = CheckpointStore(tmp_path / "t.db")

    def boom(ctx):  # type: ignore[no-untyped-def]
        raise RuntimeError("die mid-step")

    wf = Workflow("w", [Step("a", boom)])
    with pytest.raises(RuntimeError):
        WorkflowEngine(store).run(wf, "ss1")
    assert store.step_started("ss1", "a") is not None  # start was recorded
    assert store.completed_steps("ss1") == {}  # but nothing completed


def test_checkpoint_conflict_raises(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = CheckpointStore(tmp_path / "t.db")
    store.create_run("r", "wf")
    store.save(Checkpoint("r", "a", "key1", {"a": 1}, 1.0))
    store.save(Checkpoint("r", "a", "key1", {"a": 1}, 1.0))  # identical: idempotent no-op
    with pytest.raises(CheckpointConflictError):
        store.save(Checkpoint("r", "a", "key2", {"a": 2}, 2.0))


def test_run_lock_blocks_second_live_owner(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = CheckpointStore(tmp_path / "t.db")
    store.create_run("r", "wf")
    store.acquire_run_lock("r")
    try:
        other = CheckpointStore(tmp_path / "t.db")
        # Same process pid holds it, so simulate a *different* live owner:
        other._con.execute("UPDATE run_locks SET owner_pid=? WHERE run_id=?", (1, "r"))
        other._con.commit()
        assert other.lock_owner("r") == 1  # pid 1 is alive (init)
        with pytest.raises(RunLockedError):
            other.acquire_run_lock("r")
        other.close()
    finally:
        store.release_run_lock("r")
