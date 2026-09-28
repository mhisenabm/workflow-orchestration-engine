"""Create Engine database tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_engine"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflow_runtime",
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("workflow_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("workflow_input", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("error", postgresql.JSONB(), nullable=True),
    )
    op.create_table(
        "task_executions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "execution_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflow_runtime.execution_id"),
            nullable=False,
        ),
        sa.Column("node_id", sa.String(128), nullable=False),
        sa.Column("handler", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("config", postgresql.JSONB(), nullable=False),
        sa.Column("inputs", postgresql.JSONB(), nullable=False),
        sa.Column("retry_max_attempts", sa.Integer(), nullable=False),
        sa.Column("retry_delay_seconds", sa.Float(), nullable=False),
        sa.Column("timeout", sa.Integer(), nullable=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", postgresql.JSONB(), nullable=True),
        sa.UniqueConstraint("execution_id", "node_id", "attempt", name="uq_task_attempt"),
    )
    op.create_index("ix_task_retry_due", "task_executions", ["status", "available_at"])
    op.create_index("ix_task_execution_node", "task_executions", ["execution_id", "node_id"])
    op.create_table(
        "task_results",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "task_execution_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("task_executions.id"),
            nullable=False,
        ),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("node_id", sa.String(128), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("output", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task_execution_id", name="uq_task_result_task"),
    )
    op.create_index("ix_task_result_execution_node", "task_results", ["execution_id", "node_id"])
    op.create_table(
        "engine_outbox",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("message_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("message_id", name="uq_engine_outbox_message_id"),
    )
    op.create_index("ix_engine_outbox_unpublished", "engine_outbox", ["published_at", "created_at"])
    op.create_table(
        "engine_processed_messages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("consumer", sa.String(100), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("message_id", "consumer", name="uq_engine_processed"),
    )


def downgrade() -> None:
    op.drop_table("engine_processed_messages")
    op.drop_index("ix_engine_outbox_unpublished", table_name="engine_outbox")
    op.drop_table("engine_outbox")
    op.drop_index("ix_task_result_execution_node", table_name="task_results")
    op.drop_table("task_results")
    op.drop_index("ix_task_execution_node", table_name="task_executions")
    op.drop_index("ix_task_retry_due", table_name="task_executions")
    op.drop_table("task_executions")
    op.drop_table("workflow_runtime")
