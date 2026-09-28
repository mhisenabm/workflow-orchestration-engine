# Development

## Local Python environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

## Quality gates

```bash
ruff format --check .
ruff check .
mypy apps packages
pytest -m 'not integration'
```

The project uses one installable root package while preserving service folders.
Imports follow the repository boundaries, for example:

```python
from apps.engine.application.orchestrator import EngineOrchestrator
from packages.contracts import ExecuteTaskCommand
```

## Adding a Worker handler

Adding a new handler is outside frozen v1 and should start with a design change.
When approved, implement the `TaskHandler` protocol and register it in
`apps/worker/main.py`. Do not place service-specific handlers in a shared package.

## Database migrations

Create API, Engine, and Worker migrations independently:

```bash
alembic -c infrastructure/postgres/api/alembic.ini revision --autogenerate -m "..."
alembic -c infrastructure/postgres/engine/alembic.ini revision --autogenerate -m "..."
alembic -c infrastructure/postgres/worker/alembic.ini revision --autogenerate -m "..."
```

Review generated migrations before committing them.

## Configuration

Configuration uses the `WOE_` environment prefix. See `.env.example`.

Local non-Docker defaults use:

```text
API PostgreSQL    localhost:5433
Engine PostgreSQL localhost:5434
Worker PostgreSQL localhost:5435
Kafka             localhost:9092
```

Docker overrides Kafka with `kafka:9092` and uses Compose database hostnames.
