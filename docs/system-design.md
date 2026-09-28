# Workflow Orchestration Engine — Design

## 1. Scope

This document describes the implemented v1 architecture for the Event-Driven
Workflow Orchestration Engine (WOE). It is the main design document for the
submission.

WOE accepts a JSON workflow represented as a directed acyclic graph (DAG),
validates it, stores an immutable snapshot, and executes it asynchronously using
Kafka. The system supports linear chains, parallel fan-out/fan-in, JSON data
passing through templates, mocked external/LLM workers, retries, timeouts, and
idempotent duplicate handling.

The implementation deliberately excludes authentication, tenancy, real HTTP or
LLM integrations, cancellation, replay, compensation, and a general-purpose
expression language. These are not required by the challenge and would distract
from the orchestration core.

## 2. Primary goals

- A synchronous API acknowledgement with asynchronous background execution.
- Complete DAG validation before an execution can be created.
- Parallel dispatch for independent nodes.
- Correct fan-in scheduling when dependencies finish concurrently.
- Deterministic template resolution from stored ancestor outputs.
- Durable workflow, task, and result state in PostgreSQL.
- At-least-once Kafka delivery with idempotent processing.
- Horizontally scalable Worker processes.
- Clear service and database ownership.
- A Docker Compose environment that starts the complete system.

## 3. Public API contract

The challenge-defined business routes are:

```text
POST /workflow
POST /workflow/trigger/{execution_id}
GET  /workflows/{execution_id}
GET  /workflows/{execution_id}/results
```

### `POST /workflow`

Validates the submitted workflow, writes an immutable workflow snapshot, creates
an execution request, and returns:

```json
{
  "execution_id": "uuid",
  "status": "pending"
}
```

The workflow is not started by this endpoint.

### `POST /workflow/trigger/{execution_id}`

Writes one `ExecuteWorkflowCommand` to the API transactional outbox and returns
HTTP 202. Repeated calls are idempotent: the API uses `triggered_at` as the
internal guard and does not publish another command.

The public status remains `pending` until the Engine publishes
`WorkflowStarted`.

### `GET /workflows/{execution_id}`

Returns the API projection of the workflow state. Public states are:

```text
pending
running
completed
failed
```

`pending` covers both “submitted but not triggered” and “trigger accepted but
not yet started by the Engine.” The timestamp fields distinguish those phases
without adding non-required public statuses.

### `GET /workflows/{execution_id}/results`

Returns the final aggregated result only after completion. Before completion it
returns HTTP 409.

Operational routes `/health/live`, `/health/ready`, and `/metrics` are excluded
from the business API and exist only for Docker/runtime operations.

## 4. Main architecture

```text
Client
  |
  | HTTP
  v
API Service ---- API PostgreSQL
  |
  | ExecuteWorkflowCommand (Kafka)
  v
Engine Service ---- Engine PostgreSQL
  |
  | ExecuteTaskCommand (Kafka)
  v
Worker Service(s) ---- Worker PostgreSQL
  |
  | TaskStarted / TaskCompleted / TaskFailed (Kafka)
  v
Engine Service
  |
  | WorkflowStarted / WorkflowCompleted / WorkflowFailed (Kafka)
  v
API status/result projection
```

The three services have intentionally different responsibilities:

- **API:** submission, validation, immutable snapshots, trigger acknowledgement,
  and public status/result projection.
- **Engine:** authoritative runtime state, readiness detection, template
  resolution, scheduling, retries, and workflow completion/failure.
- **Worker:** handler execution and duplicate task-message protection.

This separation keeps DAG decisions in one place and allows Workers to scale
without owning workflow state.

## 5. Pragmatic Clean Architecture

Each service uses four practical layers:

```text
domain/
application/
infrastructure/
interfaces/
```

- **Domain** contains state enums and entities. It imports no FastAPI,
  SQLAlchemy, Kafka, or infrastructure clients.
- **Application** contains use cases, ports, the scheduler, template resolver,
  Worker executor, and handler registry.
- **Infrastructure** implements SQLAlchemy repositories, Kafka producers,
  outbox publishers, and mock handlers.
- **Interfaces** contains HTTP routes, Kafka consumers, and process startup.

Shared code is limited to contracts, configuration, and observability. Service
repositories and domain entities are not placed in a generic shared package.

### Trade-off

A stricter Clean Architecture implementation could create more individual
interfaces and mapping types. That would increase boilerplate without improving
the challenge’s core behavior. The chosen structure preserves dependency
inversion while remaining readable.

## 6. Workflow model and validation

A node has the following required fields:

```text
id
handler
```

Optional fields are:

```text
dependencies
config
inputs
retry
timeout
metadata
```

Supported handlers are:

```text
input
call_external_service
llm_service
output
```

The validator enforces:

- Unique node IDs.
- Existing dependency references.
- No self-dependency.
- No duplicate dependencies.
- No directed cycle.
- Exactly one `input` handler with ID `input`.
- Exactly one `output` handler with ID `output`.
- Input has no dependencies.
- Output has at least one dependency.
- Every node is reachable from input.
- Every node leads to output.
- Valid handler-specific configuration.
- Valid template syntax and ancestor-only references.

Cycle detection uses Kahn’s topological algorithm. Reachability is checked in
both forward and reverse directions.

### Trade-off: strict topology

The challenge only requires a DAG, but v1 also requires a single input and
output boundary. This makes final aggregation and workflow completion
unambiguous. Supporting multiple roots or outputs later would require an
explicit aggregation policy.

## 7. Persistence and ownership

### API database

- `workflow_snapshots`
- `workflow_executions`
- `api_outbox`
- `api_processed_messages`

The snapshot row is the parent of the execution row. The API explicitly flushes
the snapshot before inserting the execution. This is necessary because the
repository design does not expose an ORM relationship that SQLAlchemy could use
to infer mapper insertion order. Both inserts still occur in one transaction;
`flush` does not commit.

### Engine database

- `workflow_runtime`
- `task_executions`
- `task_results`
- `engine_outbox`
- `engine_processed_messages`

The Engine also explicitly flushes `workflow_runtime` before inserting its
`task_executions` children, preventing the same foreign-key ordering problem.

A unique constraint on:

```text
(execution_id, node_id, attempt)
```

prevents duplicate logical task attempts.

### Worker database

- `worker_task_receipts`
- `worker_outbox`

The Worker database exists solely to meet the requirement that the same task
message must not execute twice across replicas or restarts.

### Trade-off: three databases

Separate databases make ownership explicit and prevent accidental cross-service
writes. They increase local setup cost compared with one shared schema, but the
separation is valuable for reasoning about state and independent service
scaling. Docker Compose hides most of the setup burden.

## 8. Event contracts and Kafka topics

HTTP models are not reused as Kafka messages. All messages use a versioned
envelope containing `message_id`, `message_type`, `version`, `occurred_at`, and
`payload`.

Commands:

- `ExecuteWorkflowCommand`
- `ExecuteTaskCommand`

Events:

- `WorkflowStarted`
- `WorkflowCompleted`
- `WorkflowFailed`
- `TaskStarted`
- `TaskCompleted`
- `TaskFailed`

Topics:

```text
woe.workflow.commands
woe.task.commands
woe.task.events
woe.workflow.events
```

Task commands are keyed by `task_execution_id`, allowing independent tasks from
the same workflow to be processed by different Worker partitions. Task and
workflow events are keyed by `execution_id`, preserving per-workflow ordering
within each topic.

### Trade-off: at-least-once rather than exactly-once Kafka

WOE does not claim end-to-end exactly-once delivery. Kafka records can be
redelivered after a process crash. Correctness is achieved using:

- Processed-message tables.
- Deterministic event IDs.
- Unique database constraints.
- Transactional outboxes.
- Durable Worker receipts.

This is easier to understand and operate than distributed transactions spanning
Kafka and three PostgreSQL databases.

## 9. Transactional outbox

A state change and its outbound message are committed to the same local
database transaction. A background publisher sends unpublished rows to Kafka
and sets `published_at`.

Failure cases:

- Crash before Kafka publication: the row remains unpublished and is retried.
- Crash after Kafka accepts the record but before `published_at`: the message may
  be published again, and consumers deduplicate it.

### Trade-off

The outbox adds tables and background loops, but it closes the critical gap
between committing database state and publishing a Kafka message.

## 10. Workflow start algorithm

The Engine handles `ExecuteWorkflowCommand` in one transaction:

1. Deduplicate the command by message ID.
2. Validate the immutable workflow snapshot again at the trust boundary.
3. Insert `workflow_runtime` in `running` state.
4. Flush the runtime parent row.
5. Insert attempt-one task rows for all nodes.
6. Complete the `input` node internally.
7. Store workflow input as the input node’s JSON result.
8. Write `WorkflowStarted` to the outbox.
9. Evaluate and queue all ready nodes.
10. Commit.

Revalidation protects the Engine from malformed messages even though the API
already validated the workflow.

## 11. Readiness and scheduling

A node is ready when:

- Its latest task attempt is `pending`.
- Every dependency’s latest attempt is `completed`.

For a Worker-owned node, the Engine:

1. Loads persisted ancestor outputs.
2. Resolves templates in `config` and `inputs`.
3. Changes the task to `queued`.
4. Writes one `ExecuteTaskCommand` to the outbox.

For the Engine-owned `output` node, the Engine aggregates direct dependency
outputs by node ID, completes the workflow, and writes `WorkflowCompleted`.

## 12. Template resolution and data passing

Templates use:

```text
{{ node_id.output_key }}
```

Examples:

```text
{{ input.customer_id }}
{{ get_user.data.name }}
```

Resolution occurs in the Engine before dispatch. It recursively traverses
objects and arrays.

- A field containing only a template preserves the referenced JSON type.
- A template embedded in a larger string is converted to text.
- Objects and arrays embedded in text are encoded as compact JSON.
- Only direct or transitive ancestors may be referenced.
- Results are loaded from `task_results`, not from transient Kafka payloads or
  Worker memory.

### Trade-off: deliberately small template language

V1 supports object dot paths only. It excludes array indexes, expressions,
conditions, loops, and functions. A larger language would introduce parsing,
security, and determinism concerns beyond the challenge.

## 13. Worker implementation and idempotency

### Mock external service

`call_external_service` sleeps for a random one-to-two-second delay and returns
JSON containing the resolved URL and dummy data. It never calls the network.

### Mock LLM

`llm_service` receives an already-resolved prompt, sleeps briefly, and returns a
mock response string.

### JSON output enforcement

Every handler result is validated as a Pydantic `JsonValue` before it is stored
or published. A handler that returns a Python-only value such as an arbitrary
object produces `TaskFailed` with code `INVALID_TASK_OUTPUT`; it cannot poison a
JSONB row or Kafka contract.

### Duplicate task handling

Each legitimate task attempt has one `task_execution_id`.

The Worker atomically claims a receipt:

- Missing receipt: insert `processing` and execute.
- Completed/failed receipt: reuse the terminal result and do not execute.
- Active processing receipt: do not execute; leave the Kafka offset uncommitted
  and retry later.
- Expired processing receipt: mark that attempt failed as abandoned; a real
  retry must use a new task ID.

The executor verifies that a repeated task ID still has the same execution ID,
node ID, handler, and attempt. Reusing one task ID for a different logical task
is rejected rather than returning an unrelated stored result.

### Trade-off: persistent receipts

An in-memory set would be simpler, but it fails when Workers restart or multiple
replicas consume duplicates. PostgreSQL receipts provide the required durable
idempotency at the cost of another small database.

## 14. Fan-out, fan-in, and race safety

For `A -> B,C -> D`:

1. A completes.
2. B and C are independently queued.
3. D remains pending until both complete.
4. D receives both outputs.

When B and C finish concurrently, task-event handling locks the
`workflow_runtime` row with `SELECT ... FOR UPDATE`. Scheduling decisions for a
single workflow are therefore serialized across Engine replicas.

Inside the lock, the Engine re-reads task states and queues D only if D is still
pending and every dependency is complete. Unique task-attempt and outbox message
constraints provide additional protection.

The Engine also verifies that every task event’s execution ID, node ID, attempt,
and task ID agree with the persisted task. A mismatched event cannot mutate the
wrong workflow.

### Trade-off: workflow-level locking

Locking one runtime row is simple and makes the race argument clear. It can
serialize many task completions within a very large workflow. A future version
could use finer-grained dependency counters or advisory locks, but those designs
are harder to prove correct and are unnecessary for this scope.

## 15. Retries and timeouts

Retry defaults:

```text
max_attempts = 1
```

No retry occurs unless configured. `max_attempts` includes the first attempt.
Delay is fixed. A retry receives a new `task_execution_id` and can therefore run
without violating Worker duplicate protection. The retry finder joins the
workflow runtime and returns retries only for workflows that are still running,
so retries left behind by a parallel terminal failure are not polled forever.

Timeout is optional and expressed in whole seconds. The Worker uses
`asyncio.timeout`. A timeout becomes `TaskFailed` with code `TASK_TIMEOUT` and
follows the normal retry policy.

### Trade-off

Fixed delay and one failure class keep behavior deterministic. Exponential
backoff, jitter, and retryable/non-retryable classifications are useful future
extensions but not needed for the challenge.

## 16. Failure semantics

When a task exhausts its attempts:

1. The task remains failed.
2. The workflow becomes failed.
3. `WorkflowFailed` is written to the Engine outbox.
4. No new dependent tasks are scheduled.
5. Late events cannot return a terminal workflow to running or completed.
6. The first terminal API projection event is authoritative; a later conflicting
   terminal event cannot overwrite the stored result or error.

A terminal task event is accepted only for a task that was actually queued or
running. This prevents a malformed event from completing a task whose
dependencies were never satisfied.

There is no rollback or compensation in v1.

## 17. Status model

Public workflow states are exactly:

```text
pending
running
completed
failed
```

Internally, `triggered_at` records whether the trigger command has already been
created. This avoids exposing extra `created` or `triggered` states while keeping
trigger calls idempotent.

Engine task states are:

```text
pending
queued
running
completed
failed
retrying
```

The richer task state is internal and required for orchestration.

## 18. Core acceptance scenarios

### Scenario A — linear

`A -> B -> C`. C is never dispatched before B completes. C can resolve data
from B and any transitive ancestor such as A.

### Scenario B — fan-out/fan-in

`A -> B,C -> D`. B and C dispatch independently. D waits for both and aggregates
their JSON outputs.

### Scenario C — race

B and C complete concurrently. PostgreSQL locking, state checks, deterministic
message IDs, and unique constraints guarantee one logical D dispatch.

## 19. Testing strategy

Unit tests cover:

- DAG validation and cycle detection.
- Template resolution and type preservation.
- Linear and fan-out/fan-in execution.
- Same-time completion race behavior.
- Retry and timeout rules.
- Worker duplicate handling.
- Task/event identity mismatch rejection.
- Prevention of terminal events for pending tasks.
- Exact HTTP route and public status contracts.
- Explicit parent-before-child flush order.

PostgreSQL/Kafka integration tests cover:

- API snapshot/execution foreign-key insertion.
- Trigger deduplication.
- Engine fan-in concurrency.
- Worker receipt deduplication.
- Kafka contract serialization.
- Full Docker API-to-result flow.

## 20. Operational model

Docker Compose starts:

- Three PostgreSQL services.
- Kafka in single-node KRaft mode.
- One API.
- One Engine.
- One or more Workers.
- Separate one-shot migration services.

Workers expose no fixed host port, so this works:

```bash
docker compose up --build --scale worker=3
```

Single-node Kafka/PostgreSQL are appropriate for the challenge environment, not
for a high-availability production deployment.

## 21. Known limitations

- Docker Compose uses single-instance infrastructure.
- Only mock handlers exist.
- No cancellation, compensation, or replay.
- No array indexing in templates.
- Outbox and processed-message cleanup are not implemented.
- Engine scheduling is serialized per workflow for correctness.
- API background-loop process supervision is basic.

## 22. Alternatives considered

### One monolithic service

Rejected because it would weaken the distributed-worker and message-broker
requirements and mix HTTP, orchestration, and execution concerns.

### Direct API-to-Worker dispatch

Rejected because the Engine must own dependency readiness, templates, retries,
and fan-in state.

### In-memory state only

Rejected because race-condition, restart, and duplicate-delivery behavior would
be difficult to prove and test across replicas.

### Real external calls

Rejected because the challenge explicitly requires mocked execution.

### Exactly-once Kafka transactions

Rejected because they would not cover PostgreSQL and external task effects
without a much more complicated distributed transaction model.

### In-memory Worker idempotency

Rejected because it does not survive restarts and is not shared by replicas.

## 23. Future extensions

Possible future work, outside v1, includes real plugin integrations, richer
template expressions, cancellation, compensation, retention jobs, large result
storage, authentication, and highly available deployment manifests. None of
these changes the submitted v1 contract.
