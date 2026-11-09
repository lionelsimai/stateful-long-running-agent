"""Crash test: spawn a worker, kill -9 it mid-run, resume, verify.

Deterministic: the worker blocks *after* its 2nd checkpoint commits
and writes a ready flag; only then does this parent SIGKILL it. The
resume must then satisfy all of:

- ``skipped`` == exactly the steps checkpointed before the kill,
- the remaining steps execute,
- the append-only side-effect log contains each of the 6 steps
  exactly once (total 6 — no duplicated effect from the replay).

Any violation prints CRASH TEST FAIL and exits 1.
"""
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "src"))

from stateful_agent.engine import WorkflowEngine
from stateful_agent.store import CheckpointStore
from stateful_agent.workflows import STEP_NAMES, crash_workflow

EXPECTED_BEFORE = STEP_NAMES[:2]


def main() -> int:
    """Run the deterministic kill -9 crash test."""
    tmp = Path(tempfile.mkdtemp(prefix="crash-test-"))
    db, effects, flag = tmp / "crash.db", tmp / "effects.log", tmp / "ready.flag"
    worker = HERE / "_slow_worker.py"
    proc = subprocess.Popen(
        [sys.executable, str(worker), str(db), str(effects), str(flag)], cwd=str(ROOT))
    try:
        deadline = time.time() + 30
        while not flag.exists():
            if time.time() > deadline:
                print("CRASH TEST FAIL: worker never signalled readiness")
                return 1
            if proc.poll() is not None:
                print(f"CRASH TEST FAIL: worker exited early rc={proc.returncode}")
                return 1
            time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
    print(f"worker killed with SIGKILL (returncode={proc.returncode})")

    store = CheckpointStore(db)
    before = list(store.completed_steps("crash-run"))
    print(f"checkpoints survived kill -9: {before}")
    result = WorkflowEngine(store).run(crash_workflow(effects), "crash-run")
    print(f"resume: status={result.status} executed={result.executed} skipped={result.skipped}")
    effect_lines = effects.read_text().split() if effects.exists() else []
    print(f"side effects: {effect_lines} (total={len(effect_lines)})")

    problems = []
    if before != EXPECTED_BEFORE:
        problems.append(f"checkpoints before resume {before} != expected {EXPECTED_BEFORE}")
    if result.skipped != EXPECTED_BEFORE:
        problems.append(f"skipped {result.skipped} != steps completed before kill {EXPECTED_BEFORE}")
    if result.executed != STEP_NAMES[2:]:
        problems.append(f"executed {result.executed} != remaining {STEP_NAMES[2:]}")
    if sorted(effect_lines) != sorted(STEP_NAMES) or len(effect_lines) != len(STEP_NAMES):
        problems.append(f"side effects {effect_lines} are not exactly the 6 steps once each")
    if result.status != "completed":
        problems.append(f"final status {result.status} != completed")
    if problems:
        for problem in problems:
            print(f"CRASH TEST FAIL: {problem}")
        return 1
    print("CRASH TEST PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
