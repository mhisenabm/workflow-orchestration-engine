from prometheus_client import Counter, Gauge, Histogram

WORKFLOW_SUBMISSIONS = Counter("woe_workflow_submissions_total", "Submitted workflows")
WORKFLOW_TRANSITIONS = Counter(
    "woe_workflow_transitions_total",
    "Workflow state transitions",
    ["service", "status"],
)
TASK_TRANSITIONS = Counter(
    "woe_task_transitions_total",
    "Task state transitions",
    ["service", "status", "handler"],
)
TASK_DURATION = Histogram(
    "woe_worker_task_duration_seconds",
    "Worker handler duration",
    ["handler"],
)
OUTBOX_BACKLOG = Gauge("woe_outbox_backlog", "Unpublished outbox records", ["service"])
