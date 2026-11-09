"""SQLite checkpoint store — step-start records + one checkpoint per step.

Durability settings are explicit: WAL journal, ``synchronous=FULL``
(every commit is fsynced before it returns) and a 5s ``busy_timeout``.
Writes are *checked*: a conflicting checkpoint raises instead of
being silently ignored.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stateful_agent.errors import CheckpointConflictError, RunLockedError


@dataclass(frozen=True)
class Checkpoint:
    """A persisted step result."""

    run_id: str
    step: str
    idempotency_key: str
    result: dict[str, Any]
    created_at: float


class CheckpointStore:
    """Durable store backed by SQLite (WAL + FULL synchronous)."""

    def __init__(self, db_path: Path | str) -> None:
        """Open (or create) the store."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(self.db_path))
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=FULL")
        self._con.execute("PRAGMA busy_timeout=5000")
        self._con.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs(
              run_id TEXT PRIMARY KEY, workflow TEXT, status TEXT,
              created_at REAL, updated_at REAL);
            CREATE TABLE IF NOT EXISTS checkpoints(
              run_id TEXT, step TEXT, idempotency_key TEXT UNIQUE,
              result TEXT, created_at REAL,
              PRIMARY KEY(run_id, step));
            CREATE TABLE IF NOT EXISTS step_starts(
              run_id TEXT, step TEXT, idempotency_key TEXT, started_at REAL,
              PRIMARY KEY(run_id, step));
            CREATE TABLE IF NOT EXISTS run_locks(
              run_id TEXT PRIMARY KEY, owner_pid INTEGER, acquired_at REAL);
            """
        )
        self._con.commit()

    def close(self) -> None:
        """Close the connection."""
        self._con.close()

    # ------------------------------------------------------------- runs
    def create_run(self, run_id: str, workflow: str) -> None:
        """Create a run row if absent."""
        now = time.time()
        self._con.execute(
            "INSERT OR IGNORE INTO runs VALUES (?,?,?,?,?)",
            (run_id, workflow, "running", now, now),
        )
        self._con.commit()

    def set_status(self, run_id: str, status: str) -> None:
        """Update run status."""
        self._con.execute("UPDATE runs SET status=?, updated_at=? WHERE run_id=?",
                          (status, time.time(), run_id))
        self._con.commit()

    def status(self, run_id: str) -> str | None:
        """Return run status or None."""
        row = self._con.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return str(row[0]) if row else None

    def list_runs(self) -> list[tuple[str, str, str]]:
        """List (run_id, workflow, status)."""
        rows = self._con.execute(
            "SELECT run_id, workflow, status FROM runs ORDER BY created_at").fetchall()
        return [(str(a), str(b), str(c)) for a, b, c in rows]

    # ------------------------------------------------------- checkpoints
    def save(self, cp: Checkpoint) -> None:
        """Persist a checkpoint, checking for conflicts.

        Re-saving identical content is an idempotent no-op. Saving
        *different* content under the same (run, step) or the same
        idempotency key raises :class:`CheckpointConflictError` —
        silently ignoring it (the 0.1.0 behaviour) could mask a
        double-execution bug.
        """
        try:
            self._con.execute(
                "INSERT INTO checkpoints VALUES (?,?,?,?,?)",
                (cp.run_id, cp.step, cp.idempotency_key, json.dumps(cp.result), cp.created_at),
            )
            self._con.commit()
        except sqlite3.IntegrityError as exc:
            self._con.rollback()
            existing = self.get(cp.run_id, cp.step)
            if existing is not None:
                if existing.idempotency_key == cp.idempotency_key and existing.result == cp.result:
                    return
                raise CheckpointConflictError(
                    f"checkpoint conflict for {cp.run_id}/{cp.step}") from exc
            raise CheckpointConflictError(
                f"idempotency key already used by another step: {cp.idempotency_key}") from exc

    def get(self, run_id: str, step: str) -> Checkpoint | None:
        """Load one checkpoint."""
        row = self._con.execute(
            "SELECT run_id, step, idempotency_key, result, created_at FROM checkpoints "
            "WHERE run_id=? AND step=?", (run_id, step)).fetchone()
        if row is None:
            return None
        return Checkpoint(str(row[0]), str(row[1]), str(row[2]), json.loads(row[3]), float(row[4]))

    def completed_steps(self, run_id: str) -> dict[str, dict[str, Any]]:
        """Return {step: result} for all *completed* (checkpointed) steps."""
        rows = self._con.execute(
            "SELECT step, result FROM checkpoints WHERE run_id=? ORDER BY created_at",
            (run_id,)).fetchall()
        return {str(s): json.loads(r) for s, r in rows}

    # ------------------------------------------------------- step starts
    def record_step_start(self, run_id: str, step: str, idempotency_key: str) -> None:
        """Record that a step started, BEFORE its function runs.

        Same checked-write discipline as :meth:`save`: an identical
        re-record is a no-op (a resumed step re-records its start with
        the same key); a different key for the same step raises.
        """
        try:
            self._con.execute(
                "INSERT INTO step_starts VALUES (?,?,?,?)",
                (run_id, step, idempotency_key, time.time()),
            )
            self._con.commit()
        except sqlite3.IntegrityError as exc:
            self._con.rollback()
            row = self._con.execute(
                "SELECT idempotency_key FROM step_starts WHERE run_id=? AND step=?",
                (run_id, step)).fetchone()
            if row is not None and str(row[0]) == idempotency_key:
                return
            raise CheckpointConflictError(
                f"step-start conflict for {run_id}/{step}") from exc

    def step_started(self, run_id: str, step: str) -> str | None:
        """Return the idempotency key a step started with, or None."""
        row = self._con.execute(
            "SELECT idempotency_key FROM step_starts WHERE run_id=? AND step=?",
            (run_id, step)).fetchone()
        return str(row[0]) if row else None

    # ------------------------------------------------------------ locking
    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True  # exists, owned by someone else
        return True

    def acquire_run_lock(self, run_id: str) -> None:
        """Take the per-run lock so two processes cannot run it at once.

        A lock left by a *dead* process (e.g. after ``kill -9``) is
        reclaimed; a lock held by a live process raises
        :class:`RunLockedError`.
        """
        pid = os.getpid()
        row = self._con.execute(
            "SELECT owner_pid FROM run_locks WHERE run_id=?", (run_id,)).fetchone()
        if row is not None:
            owner = int(row[0])
            if owner == pid:
                return
            if self._pid_alive(owner):
                raise RunLockedError(f"run {run_id} is locked by live pid {owner}")
            self._con.execute("DELETE FROM run_locks WHERE run_id=?", (run_id,))
        self._con.execute("INSERT INTO run_locks VALUES (?,?,?)", (run_id, pid, time.time()))
        self._con.commit()

    def release_run_lock(self, run_id: str) -> None:
        """Release the per-run lock if this process holds it."""
        self._con.execute("DELETE FROM run_locks WHERE run_id=? AND owner_pid=?",
                          (run_id, os.getpid()))
        self._con.commit()

    def lock_owner(self, run_id: str) -> int | None:
        """Return the pid holding the run lock, or None."""
        row = self._con.execute(
            "SELECT owner_pid FROM run_locks WHERE run_id=?", (run_id,)).fetchone()
        return int(row[0]) if row else None
