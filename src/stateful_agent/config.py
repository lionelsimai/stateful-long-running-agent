"""Environment configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    """Runtime config from ``STATEFUL_*`` env vars."""

    db_path: Path = Path("stateful.db")

    @classmethod
    def from_env(cls) -> Config:
        """Build from environment."""
        return cls(db_path=Path(os.environ.get("STATEFUL_DB", "stateful.db")))
