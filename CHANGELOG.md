# Changelog

## [0.1.1] - 2026-11-09
### Fixed (world-class remediation after adversarial audit)
- Deterministic crash test: the worker now blocks after its 2nd checkpoint
  commits and signals readiness via a flag file before being SIGKILLed
  (the old test slept 1.2s and could pass while resuming from scratch).
  The test — now also a pytest subprocess test, passing 5/5 runs — fails
  loudly unless resume skips exactly the committed steps and the
  side-effect log holds each of the 6 steps exactly once.
- Idempotency keys are derived from canonicalised context *values* (was:
  key names only), are recorded in a step-start row *before* the step
  runs, and are passed into the step via `ctx["_idempotency_key"]`; demo
  steps use the key to suppress duplicate effects.
- Claims downgraded to the honest contract everywhere: at-least-once
  execution with idempotent replay. "Effectively-once" removed.
- SQLite: `synchronous=FULL` + `busy_timeout`; checkpoint writes are
  checked and raise `CheckpointConflictError` on conflicting content
  (was: silent `INSERT OR IGNORE`).
- Per-run locking: a second live process cannot execute the same run
  (`RunLockedError`); dead owners' locks are reclaimed and stale
  `running` runs are reconciled on resume.

### Added
- Real topological execution from `depends_on` (list order no longer
  needs to respect dependencies); cycle detection at validation.

## [0.1.0] - 2026-11-09
### Added
- SQLite checkpoint store, idempotency keys, step DAG engine
- Demo workflow, run/resume/status CLI, kill -9 crash test
- Benchmarks, CI, Temporal mapping docs
