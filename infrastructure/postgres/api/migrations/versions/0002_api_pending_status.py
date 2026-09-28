"""Normalize public workflow status to pending/running/completed/failed."""

from alembic import op

revision = "0002_api_pending_status"
down_revision = "0001_api"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE workflow_executions
        SET status = 'pending'
        WHERE status IN ('created', 'triggered')
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE workflow_executions
        SET status = CASE
            WHEN triggered_at IS NULL THEN 'created'
            ELSE 'triggered'
        END
        WHERE status = 'pending'
        """
    )
