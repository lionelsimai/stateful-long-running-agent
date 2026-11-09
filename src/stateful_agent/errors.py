"""Structured errors."""


class WorkflowError(Exception):
    """Base workflow error."""


class RunNotFoundError(WorkflowError):
    """Raised when a run id does not exist."""


class StepFailedError(WorkflowError):
    """Raised when a step fails non-recoverably."""


class SimulatedCrashError(WorkflowError):
    """Raised by tests/examples to simulate kill -9 (process death)."""


class RunLockedError(WorkflowError):
    """Raised when another live process already holds the run's lock."""


class CheckpointConflictError(WorkflowError):
    """Raised when a checkpoint write conflicts with different content."""
