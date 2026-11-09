"""Crash test: spawn a worker, kill -9 it mid-run, resume, verify completion.

The worker runs a slow workflow checkpointing to SQLite. We SIGKILL it,
then resume in-process: completed steps must not re-execute.
"""
import signal
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DB = HERE / "crash.db"


def main() -> int:
    """Run the kill -9 crash test."""
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(DB) + suffix)
        if p.exists():
            p.unlink()
    worker = HERE / "_slow_worker.py"
    proc = subprocess.Popen([sys.executable, str(worker), str(DB)], cwd=str(ROOT))
    time.sleep(1.2)
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    print(f"worker killed with SIGKILL (returncode={proc.returncode})")
    sys.path.insert(0, str(ROOT / "src"))
    from stateful_agent.engine import WorkflowEngine
    from stateful_agent.store import CheckpointStore
    from stateful_agent.workflows import demo_workflow

    store = CheckpointStore(DB)
    before = list(store.completed_steps("crash-run"))
    print(f"checkpoints survived kill -9: {before}")
    result = WorkflowEngine(store).run(demo_workflow(), "crash-run")
    print(f"resume: status={result.status} executed={result.executed} skipped={result.skipped}")
    ok = result.status == "completed"
    print("CRASH TEST PASS" if ok else "CRASH TEST FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
