"""Store + engine unit tests."""
import pytest

from stateful_agent.engine import Step, Workflow, WorkflowEngine, idempotency_key
from stateful_agent.errors import SimulatedCrashError
from stateful_agent.store import CheckpointStore


def make_store(tmp_path):  # type: ignore[no-untyped-def]
    return CheckpointStore(tmp_path / "t.db")


def test_store_roundtrip(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = make_store(tmp_path)
    store.create_run("r1", "wf")
    assert store.status("r1") == "running"
    assert store.status("missing") is None
    store.set_status("r1", "completed")
    assert store.list_runs()[0][2] == "completed"


def test_idempotency_key_stable() -> None:
    assert idempotency_key("r", "s", {"a": 1}) == idempotency_key("r", "s", {"a": 1})
    assert idempotency_key("r", "s", {"a": 1}) != idempotency_key("r", "s", {"a": 2})


def test_run_completes_and_checkpoints_every_step(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = make_store(tmp_path)
    wf = Workflow("w", [Step("a", lambda c: {"a": 1}), Step("b", lambda c: {"b": 2}, ("a",))])
    result = WorkflowEngine(store).run(wf, "run1")
    assert result.status == "completed" and result.executed == ["a", "b"]
    assert set(store.completed_steps("run1")) == {"a", "b"}


def test_resume_after_crash_executes_each_step_once(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = make_store(tmp_path)
    calls: list[str] = []
    state = {"crash": True}

    def flaky(ctx):  # type: ignore[no-untyped-def]
        if state["crash"]:
            state["crash"] = False
            raise SimulatedCrashError("kill -9")
        calls.append("b")
        return {"b": 2}

    wf = Workflow("w", [Step("a", lambda c: calls.append("a") or {"a": 1}), Step("b", flaky, ("a",))])
    engine = WorkflowEngine(store)
    with pytest.raises(SimulatedCrashError):
        engine.run(wf, "runX")
    assert store.status("runX") == "interrupted"
    result = engine.run(wf, "runX")
    assert result.status == "completed"
    assert result.skipped == ["a"] and result.executed == ["b"]
    assert calls == ["a", "b"]  # a not re-executed


def test_dag_validation_edge_cases() -> None:
    with pytest.raises(ValueError):
        Workflow("w", [Step("b", lambda c: {}, ("missing",))]).validate()
    with pytest.raises(ValueError):
        Workflow("w", [Step("a", lambda c: {}), Step("a", lambda c: {})]).validate()


def test_non_dict_step_output_rejected(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = make_store(tmp_path)
    wf = Workflow("w", [Step("a", lambda c: "bad")])  # type: ignore[arg-type,return-value]
    with pytest.raises(TypeError):
        WorkflowEngine(store).run(wf, "bad1")
