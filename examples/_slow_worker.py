"""Crash-test worker: runs the crash workflow, then blocks mid-run.

Deterministic by construction — no timing races. The worker uses the
engine's ``on_step_completed`` hook: after the 2nd step's checkpoint
has *committed*, it writes the ready-flag file and blocks forever.
The parent waits for the flag, sends SIGKILL, and knows exactly which
checkpoints must have survived: the first two steps.

Usage: _slow_worker.py <db_path> <effects_path> <flag_path>
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stateful_agent.engine import WorkflowEngine
from stateful_agent.store import CheckpointStore
from stateful_agent.workflows import crash_workflow

BLOCK_AFTER = 2


def main() -> int:
    """Run until blocked after step 2, then wait to be killed."""
    db, effects, flag = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    store = CheckpointStore(db)
    completed: list[str] = []

    def on_step(name: str) -> None:
        completed.append(name)
        if len(completed) == BLOCK_AFTER:
            flag.write_text(name)
            while True:  # blocked mid-run; parent will SIGKILL us
                time.sleep(60)

    WorkflowEngine(store).run(crash_workflow(effects), "crash-run",
                              on_step_completed=on_step)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
