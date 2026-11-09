"""CLI: run, resume, status, list."""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

from stateful_agent.config import Config
from stateful_agent.engine import WorkflowEngine
from stateful_agent.errors import RunNotFoundError, WorkflowError
from stateful_agent.store import CheckpointStore
from stateful_agent.workflows import demo_workflow


def _engine(db: str | None) -> tuple[WorkflowEngine, CheckpointStore]:
    path = Path(db) if db else Config.from_env().db_path
    store = CheckpointStore(path)
    return WorkflowEngine(store), store


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="stateful-agent", description="Durable checkpointed workflows.")
    parser.add_argument("--db", default=None, help="SQLite checkpoint DB path")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run_p = sub.add_parser("run", help="Run (or resume) the demo workflow")
    run_p.add_argument("--run-id", default=None)
    resume_p = sub.add_parser("resume", help="Resume a run by id")
    resume_p.add_argument("run_id")
    status_p = sub.add_parser("status", help="Show run status + steps")
    status_p.add_argument("run_id")
    sub.add_parser("list", help="List runs")
    args = parser.parse_args(argv)
    engine, store = _engine(args.db)
    try:
        if args.cmd == "list":
            for run_id, wf, status in store.list_runs():
                print(f"{run_id}  {wf}  {status}")
            return 0
        if args.cmd == "status":
            status = store.status(args.run_id)
            if status is None:
                raise RunNotFoundError(f"run not found: {args.run_id}")
            steps = store.completed_steps(args.run_id)
            print(f"run={args.run_id} status={status} steps={','.join(steps) or '-'}")
            return 0
        run_id = args.run_id if args.cmd == "resume" else (args.run_id or uuid.uuid4().hex[:12])
        result = engine.run(demo_workflow(), run_id)
        print(f"run={run_id} status={result.status} executed={','.join(result.executed) or '-'} "
              f"skipped={','.join(result.skipped) or '-'}")
        return 0
    except WorkflowError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
