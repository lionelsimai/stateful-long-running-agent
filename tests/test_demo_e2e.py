"""Demo workflow + CLI end-to-end tests."""
from stateful_agent.cli import main
from stateful_agent.engine import WorkflowEngine
from stateful_agent.store import CheckpointStore
from stateful_agent.workflows import demo_workflow


def test_demo_workflow_completes(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = CheckpointStore(tmp_path / "d.db")
    result = WorkflowEngine(store).run(demo_workflow(), "demo1")
    assert result.status == "completed" and len(result.executed) == 6


def test_e2e_cli_run_status_resume(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    db = str(tmp_path / "cli.db")
    assert main(["--db", db, "run", "--run-id", "e2e1"]) == 0
    assert "status=completed" in capsys.readouterr().out
    assert main(["--db", db, "status", "e2e1"]) == 0
    assert "steps=ingest,clean,chunk,embed,summarise,publish" in capsys.readouterr().out
    assert main(["--db", db, "resume", "e2e1"]) == 0
    out = capsys.readouterr().out
    assert "skipped=ingest" in out  # resume is a no-op re-run, nothing re-executes


def test_cli_unknown_run_returns_2(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    assert main(["--db", str(tmp_path / "x.db"), "status", "nope"]) == 2
