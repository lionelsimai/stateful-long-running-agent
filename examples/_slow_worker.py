"""Slow worker used only by crash_test.py (killed mid-run on purpose)."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stateful_agent.engine import Step, Workflow, WorkflowEngine
from stateful_agent.store import CheckpointStore


def slow(name: str) -> Step:
    """Make a step that sleeps, so SIGKILL lands mid-workflow."""

    def fn(ctx):  # type: ignore[no-untyped-def]
        time.sleep(0.8)
        return {name: True}

    return Step(name=name, fn=fn)


if __name__ == "__main__":
    store = CheckpointStore(sys.argv[1])
    wf = Workflow("slow", [slow(n) for n in
                           ["ingest", "clean", "chunk", "embed", "summarise", "publish"]])
    # Reuse demo step names so resume with demo_workflow aligns by name.
    WorkflowEngine(store).run(wf, "crash-run")
