"""SQLite checkpoint store — one checkpoint after every step."""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Checkpoint:
    """A persisted step result."""

    run_id: str
    step: str
    idempotency_key: str
    result: dict[str, Any]
    created_at: float


class CheckpointStore:
    """Durable store backed by SQLite (WAL mode)."""

    def __init__(self, db_path: Path | str) -> None:
        """Open (or create) the store."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(self.db_path))
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs(
              run_id TEXT PRIMARY KEY, workflow TEXT, status TEXT,
              created_at REAL, updated_at REAL);
            CREATE TABLE IF NOT EXISTS checkpoints(
              run_id TEXT, step TEXT, idempotency_key TEXT UNIQUE,
              result TEXT, created_at REAL,
              PRIMARY KEY(run_id, step));
            """
        )
        self._con.commit()

    def close(self) -> None:
        """Close the connection."""
        self._con.close()

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
        rows = self._con.execute("SELECT run_id, workflow, status FROM runs ORDER BY created_at").fetchall()
        return [(str(a), str(b), str(c)) for a, b, c in rows]

    def save(self, cp: Checkpoint) -> None:
        """Persist a checkpoint (idempotent on idempotency_key)."""
        self._con.execute(
            "INSERT OR IGNORE INTO checkpoints VALUES (?,?,?,?,?)",
            (cp.run_id, cp.step, cp.idempotency_key, json.dumps(cp.result), cp.created_at),
        )
        self._con.commit()

    def get(self, run_id: str, step: str) -> Checkpoint | None:
        """Load one checkpoint."""
        row = self._con.execute(
            "SELECT run_id, step, idempotency_key, result, created_at FROM checkpoints "
            "WHERE run_id=? AND step=?", (run_id, step)).fetchone()
        if row is None:
            return None
        return Checkpoint(str(row[0]), str(row[1]), str(row[2]), json.loads(row[3]), float(row[4]))

    def completed_steps(self, run_id: str) -> dict[str, dict[str, Any]]:
        """Return {step: result} for all checkpointed steps."""
        rows = self._con.execute(
            "SELECT step, result FROM checkpoints WHERE run_id=? ORDER BY created_at", (run_id,)).fetchall()
        return {str(s): json.loads(r) for s, r in rows}
