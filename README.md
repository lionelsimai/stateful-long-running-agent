# Stateful Long-Running Agent

![CI](https://github.com/lionelsimai/stateful-long-running-agent/actions/workflows/ci.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)

**Temporal-checkpointed workflows that survive crashes — resume a 24h task cleanly after `kill -9`.**

**Target:** 24h task resumes cleanly after kill -9.
**Shows:** You build durable systems not fragile demos.

## Problem → Solution
Long agent jobs die the way all long jobs die: deploy, OOM, laptop sleep, `kill -9` — and a naive agent restarts from zero, re-paying and re-doing hours of work (or worse, double-charging a side effect). This engine checkpoints to SQLite after *every* step, keys each step idempotently, and resumes exactly where it stopped. No Temporal server needed — the durable-execution pattern, implemented locally, with the Temporal mapping documented.

## Architecture
```mermaid
flowchart LR
  CLI[CLI run/resume/status] --> E[WorkflowEngine]
  E --> V{Checkpoint exists?}
  V -->|yes| SKIP[Skip + restore output]
  V -->|no| RUN[Run step]
  RUN --> C[(SQLite WAL checkpoint<br/>+ idempotency key)]
  C --> E
```
Full detail + Temporal mapping table: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Quickstart (<2 min, offline)
```bash
git clone https://github.com/lionelsimai/stateful-long-running-agent && cd stateful-long-running-agent
pip install -e .
stateful-agent --db demo.db run --run-id demo1
stateful-agent --db demo.db status demo1
stateful-agent --db demo.db resume demo1   # no-op: everything already checkpointed
python examples/crash_test.py              # the real kill -9 test
python -m pytest -q
```

## Real demo output
```text
$ stateful-agent --db demo.db run --run-id demo1
run=demo1 status=completed executed=ingest,clean,chunk,embed,summarise,publish skipped=-

$ python examples/crash_test.py
worker killed with SIGKILL (returncode=-9)
checkpoints survived kill -9: ['ingest']
resume: status=completed executed=['clean', 'chunk', 'embed', 'summarise', 'publish'] skipped=['ingest']
CRASH TEST PASS
```

## Benchmarks
Headline metric is binary and verified, not estimated: a worker process is spawned, SIGKILLed mid-workflow, and the run must complete on resume with **zero re-executed steps**.
```bash
python benchmarks/benchmark.py   # wraps the kill -9 crash test
# target: task resumes cleanly after kill -9 -> PASS
```
Simulated steps stand in for a 24h task's hours-long steps — the checkpoint/resume mechanics are identical at any step duration, because durability lives in the store, not the step.

## Honest limitations
- Single-process, single-writer SQLite — right for one host, not a distributed fleet (that's the step where the documented Temporal mapping takes over).
- Side effects outside the step's returned dict are the caller's responsibility: make step functions idempotent at the external system too, using the same key.
- No built-in retries/backoff policy yet (on the roadmap).

## Roadmap
- [ ] Retry policies with exponential backoff per step
- [ ] Human-approval step type (pause until approved, durable across restarts)
- [ ] Temporal worker adapter with the same workflow definitions
- [ ] Checkpoint encryption at rest

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md). MIT — [LICENSE](LICENSE). By Lionel Sim, The AI Capitol.
