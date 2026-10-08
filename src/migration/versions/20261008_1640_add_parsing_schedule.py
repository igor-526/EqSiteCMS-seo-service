"""Add ParsingSchedule model.

Revision ID: 20261008_1640
Revises: None
Create Date: 2026-10-08 16:40:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20261008_1640"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create parsing_schedules table."""
    op.create_table(
        "parsing_schedules",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("site_id", sa.Integer(), nullable=False),
        sa.Column("parser_type", sa.String(100), nullable=False),
        sa.Column("cron_expression", sa.String(100), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("site_id", "parser_type", name="uq_site_parser"),
    )
    
    # Create indexes
    op.create_index(
        "ix_parsing_schedules_site_id",
        "parsing_schedules",
        ["site_id"],
    )
    op.create_index(
        "ix_parsing_schedules_is_active",
        "parsing_schedules",
        ["is_active"],
    )


def downgrade() -> None:
    """Drop parsing_schedules table."""
    op.drop_index("ix_parsing_schedules_is_active", table_name="parsing_schedules")
    op.drop_index("ix_parsing_schedules_site_id", table_name="parsing_schedules")
    op.drop_table("parsing_schedules")
