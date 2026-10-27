"""Parse command dispatch ledger."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "20261028_0003"
down_revision = "20261023_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parse_dispatches",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("equestrian_id", pg.UUID(as_uuid=True), nullable=False),
        sa.Column("trigger", sa.String(16), nullable=False),
        sa.Column("schedule_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error_code", sa.String(32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("trigger IN ('schedule', 'manual')", name="ck_parse_dispatches_trigger"),
        sa.CheckConstraint("status IN ('pending', 'published', 'failed')", name="ck_parse_dispatches_status"),
        sa.CheckConstraint("attempts > 0", name="ck_parse_dispatches_attempts"),
        sa.CheckConstraint(
            "(trigger = 'schedule' AND schedule_date IS NOT NULL) OR (trigger = 'manual' AND schedule_date IS NULL)",
            name="ck_parse_dispatches_schedule_date",
        ),
    )
    op.create_index(
        "ux_parse_dispatches_schedule",
        "parse_dispatches",
        ["provider", "equestrian_id", "schedule_date"],
        unique=True,
        postgresql_where=sa.text("trigger = 'schedule'"),
    )


def downgrade() -> None:
    op.drop_index("ux_parse_dispatches_schedule", table_name="parse_dispatches")
    op.drop_table("parse_dispatches")
