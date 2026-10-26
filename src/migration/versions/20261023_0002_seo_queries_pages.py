"""Tenant-scoped SEO queries, pages and normalization blacklists."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "20261023_0002"
down_revision = "20260710_0001"
branch_labels = None
depends_on = None


def _base_columns() -> list[sa.Column]:
    return [
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("equestrian_id", pg.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    ]


def upgrade() -> None:
    # Keep the shared extension out of transient tenant/test schemas.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
    for table, field in (("search_queries", "query"), ("pages", "page")):
        op.create_table(
            table,
            *_base_columns(),
            sa.Column(field, sa.Text(), nullable=False),
            sa.Column(f"{field}_hash", pg.BYTEA(), nullable=False),
        )
        op.create_index(
            f"ux_{table}_equestrian_id_{field}_hash", table, ["equestrian_id", f"{field}_hash"], unique=True
        )
        op.create_index(
            f"ix_{table}_equestrian_id_created_at",
            table,
            ["equestrian_id", sa.text("created_at DESC"), sa.text("id DESC")],
        )
        op.create_index(
            f"ix_{table}_{field}_trgm", table, [field], postgresql_using="gin", postgresql_ops={field: "gin_trgm_ops"}
        )
    for table in ("blacklisted_query_params", "blacklisted_hashes"):
        op.create_table(table, *_base_columns(), sa.Column("value", sa.String(255), nullable=False))
        op.create_index(f"ux_{table}_equestrian_id_value", table, ["equestrian_id", "value"], unique=True)


def downgrade() -> None:
    for table in ("blacklisted_hashes", "blacklisted_query_params"):
        op.drop_index(f"ux_{table}_equestrian_id_value", table_name=table)
        op.drop_table(table)
    for table, field in (("pages", "page"), ("search_queries", "query")):
        op.drop_index(f"ix_{table}_{field}_trgm", table_name=table)
        op.drop_index(f"ix_{table}_equestrian_id_created_at", table_name=table)
        op.drop_index(f"ux_{table}_equestrian_id_{field}_hash", table_name=table)
        op.drop_table(table)
