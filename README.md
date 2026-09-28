# Workflow Orchestration Engine (WOE)

WOE is a Python 3.12 distributed DAG workflow orchestrator built with FastAPI,
PostgreSQL, Kafka, SQLAlchemy 2 async, Pydantic v2, and Docker Compose.

This repository implements the frozen v1 scope:

- Handler-based DAG definitions.
- Exactly four workflow business endpoints.
- Separate API, Engine, and Worker services.
- Mock external-service and mock LLM handlers; no real external APIs are called.
- Engine-side `{{ node.path }}` template resolution.
- Parallel fan-out and dependency-based fan-in.
- Concurrency-safe scheduling of a dependent node exactly once per attempt.
- Persistent Worker idempotency using `task_execution_id`.
- Retries, task timeouts, explicit Kafka contracts, and transactional outboxes.

## Repository layout

```text
apps/
  api/
  engine/
  worker/
packages/
  contracts/
  config/
  observability/
infrastructure/
  kafka/
  postgres/
tests/
  unit/
  integration/
docs/
```

## Quick start

Requirements:

- Docker with Docker Compose.
- Ports `8000`, `29092`, and `5433`-`5435` available.

Start three Worker replicas:

```bash
docker compose up --build --scale worker=3
```

The API becomes available at `http://localhost:8000`.

## Submit the frozen example DAG

```bash
curl --location 'http://localhost:8000/workflow' \
  --header 'Content-Type: application/json' \
  --data '{
    "name": "Parallel API Fetcher",
    "dag": {
      "nodes": [
        {
          "id": "input",
          "handler": "input",
          "dependencies": []
        },
        {
          "id": "get_user",
          "handler": "call_external_service",
          "dependencies": ["input"],
          "config": {
            "url": "http://localhost:8911/document/policy/list"
          }
        },
        {
          "id": "get_posts",
          "handler": "call_external_service",
          "dependencies": ["input"],
          "config": {
            "url": "http://localhost:8911/document/policy/list"
          }
        },
        {
          "id": "get_comments",
          "handler": "call_external_service",
          "dependencies": ["input"],
          "config": {
            "url": "http://localhost:8911/document/policy/list"
          }
        },
        {
          "id": "output",
          "handler": "output",
          "dependencies": ["get_user", "get_posts", "get_comments"]
        }
      ]
    }
  }'
```

The response is:

```json
{
  "execution_id": "<uuid>",
  "status": "pending"
}
```

The supplied URLs are not called. Each `call_external_service` task sleeps for
one to two seconds and returns dummy JSON.

Trigger the execution. The response remains `pending` until the Engine emits
`WorkflowStarted`; repeated trigger calls do not publish another command.

```bash
curl --request POST \
  'http://localhost:8000/workflow/trigger/<execution_id>'
```

Read status:

```bash
curl 'http://localhost:8000/workflows/<execution_id>'
```

Read results after completion:

```bash
curl 'http://localhost:8000/workflows/<execution_id>/results'
```

## Business API

```text
POST /workflow
POST /workflow/trigger/{execution_id}
GET  /workflows/{execution_id}
GET  /workflows/{execution_id}/results
```

Operational endpoints used by Docker:

```text
GET /health/live
GET /health/ready
GET /metrics
```

No authentication, tenancy, rate limiting, idempotency HTTP parameter, or
additional workflow lifecycle endpoint is included in v1.

## Supported handlers

| Handler | Owner | Behavior |
|---|---|---|
| `input` | Engine | Immediately outputs the workflow input JSON. |
| `call_external_service` | Worker | Sleeps 1-2 seconds and returns dummy JSON. |
| `llm_service` | Worker | Returns a mock string from an already-resolved prompt. |
| `output` | Engine | Aggregates direct dependency outputs by node ID. |

## Template resolution

Templates are resolved by the Engine before task dispatch:

```json
{
  "prompt": "Summarize {{ get_user.data.message }} for {{ input.requested_by }}"
}
```

A full-field template preserves its JSON type:

```json
{
  "items": "{{ get_posts.data.items }}"
}
```

If the referenced value is an array, `items` remains an array. V1 supports dot
paths through JSON objects; array indexing and expression syntax are excluded.

## Development commands

```bash
python -m pip install -e '.[dev]'
ruff format --check .
ruff check .
mypy apps packages
pytest -m 'not integration'
```

Run the Docker end-to-end test after the stack is healthy:

```bash
./scripts/run-integration.sh
```

Detailed documentation:

- [Design](DESIGN.md)
- [System design mirror](docs/system-design.md)
- [Development](docs/development.md)
- [Testing](docs/testing.md)
- [Docker](docs/docker.md)
