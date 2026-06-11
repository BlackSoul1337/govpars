"""Tune crawl frontier autovacuum for small and large queues.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE crawl_frontier SET (
            autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_vacuum_threshold = 50,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_analyze_threshold = 50
        )
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE crawl_frontier SET (
            autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_vacuum_threshold = 1000,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_analyze_threshold = 1000
        )
        """
    )
