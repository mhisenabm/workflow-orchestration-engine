# Testing

## Unit tests

```bash
pytest -m 'not integration'
```

Coverage includes:

- Schema compatibility and DAG validation.
- Cycle, dependency, handler, and template-reference errors.
- Recursive template resolution and JSON type preservation.
- Mock external and LLM handlers.
- Worker handler registry.
- Persistent-idempotency behavior through an in-memory repository contract.
- Retries and Worker timeouts.
- API submission, explicit parent-row flush order, and duplicate trigger behavior.
- Exact challenge POST routes and public status vocabulary.
- Workflow projection terminal-event ordering.
- Contract serialization and envelope type/payload matching.
- Linear, fan-out/fan-in, and concurrent fan-in scenarios.
- Task-event identity validation and out-of-order Worker events.
- JSON-only Worker output enforcement.
- API readiness detection for stopped critical background tasks.

The race test invokes B and C completion processing concurrently and asserts that
only one D attempt and one D command are created.

## Docker integration test

```bash
./scripts/run-integration.sh
```

The script starts PostgreSQL, Kafka, API, Engine, and three Worker replicas, then
runs `tests/integration/test_docker_e2e.py` in the Compose network.

The Docker suite:

1. Submits a workflow containing two parallel mock external nodes.
2. Triggers it and verifies the final result through the public API.
3. Exercises API, Engine, and Worker repositories against separate PostgreSQL test
   databases.
4. Runs the B/C concurrent-completion race against real PostgreSQL row locks and
   uniqueness constraints.
5. Verifies a Kafka contract round trip on an isolated test topic.
6. Verifies persistent Worker deduplication.

No real external API or LLM provider is called.

## Why the Worker tests inject sleepers

Production mock external tasks must sleep one to two seconds. Unit tests inject a
fake sleeper and deterministic delay so the behavior can be verified without
slowing the suite.


## Continuous integration

The GitHub Actions workflow has two jobs:

1. `quality` runs formatting, linting, strict typing, all unit tests with warnings
   treated as errors, bytecode compilation, and all three Alembic head checks on
   Python 3.12.
2. `integration` starts the complete Docker Compose stack with three Worker
   replicas and executes the PostgreSQL, Kafka, race-condition, deduplication,
   and end-to-end tests.
