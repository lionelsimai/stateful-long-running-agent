"""Example: run the demo workflow, then resume it (no-op)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stateful_agent.engine import WorkflowEngine
from stateful_agent.store import CheckpointStore
from stateful_agent.workflows import demo_workflow


def main() -> int:
    """Run the example."""
    store = CheckpointStore(Path(__file__).parent / "demo.db")
    engine = WorkflowEngine(store)
    r = engine.run(demo_workflow(), "example-1")
    print(f"first: status={r.status} executed={r.executed}")
    r2 = engine.run(demo_workflow(), "example-1")
    print(f"resume: status={r2.status} executed={r2.executed} skipped={r2.skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
