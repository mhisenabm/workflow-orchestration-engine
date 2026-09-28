# Docker Compose

## Services

```text
api-postgres
engine-postgres
worker-postgres
kafka
kafka-init
api-migrate
engine-migrate
worker-migrate
api
engine
worker
```

The additional Worker PostgreSQL database is required by the frozen durable
idempotency requirement. It stores only Worker task receipts and Worker outbox
records.

## Start

```bash
docker compose up --build --scale worker=3
```

The Worker service has no fixed host port, so scaling does not create port
conflicts.

## Stop and remove data

```bash
docker compose down -v
```

## Topics

`kafka-init` creates:

```text
woe.workflow.commands  3 partitions
woe.task.commands      6 partitions
woe.task.events        6 partitions
woe.workflow.events    3 partitions
```

The Compose stack uses the official `apache/kafka:3.9.1` image in single-node KRaft mode. The single-broker setup is for development and testing, not high availability.

## Migrations

One-shot `api-migrate`, `engine-migrate`, and `worker-migrate` services run Alembic
before the application services start. This avoids concurrent migrations when the
Worker service is scaled. Database ownership remains separated.

## Integration profile

```bash
docker compose --profile test run --rm integration-tests
```

The recommended wrapper is:

```bash
./scripts/run-integration.sh
```

## Metrics

The API exposes `/metrics` on port 8000. Engine and Worker processes expose
Prometheus metrics internally on ports 9101 and 9102. Worker replicas reuse port
9102 inside their own containers, so horizontal scaling has no host-port conflict.
