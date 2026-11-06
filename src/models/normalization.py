"""Durable normalization state. No runtime paths are enabled by these tables."""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from utils.basemodel import metadata


def tenant() -> Column:
    return Column("equestrian_id", PG_UUID(as_uuid=True), nullable=False)


def operation() -> Column:
    return Column("operation_id", PG_UUID(as_uuid=True), nullable=False)


def revision_fk() -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        ["equestrian_id", "operation_id"],
        ["normalization_revisions.equestrian_id", "normalization_revisions.operation_id"],
    )


normalization_tenants = Table(
    "normalization_tenants",
    metadata,
    Column("equestrian_id", PG_UUID(as_uuid=True), primary_key=True),
    Column("last_revision", BigInteger, nullable=False, server_default="0"),
    CheckConstraint("last_revision >= 0", name="ck_normalization_tenants_revision"),
)
normalization_revisions = Table(
    "normalization_revisions",
    metadata,
    tenant(),
    operation(),
    Column("revision", BigInteger, nullable=False),
    Column("status", String(32), nullable=False, server_default="pending"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("cleanup_operation_id", PG_UUID(as_uuid=True)),
    Column("cleanup_seq", BigInteger),
    Column("part_count", Integer),
    Column("last_error_code", String(64)),
    UniqueConstraint("equestrian_id", "operation_id", name="uq_normalization_revision_operation"),
    UniqueConstraint("operation_id", name="uq_normalization_operation"),
    UniqueConstraint("equestrian_id", "revision", name="uq_normalization_revision_number"),
    ForeignKeyConstraint(["equestrian_id"], ["normalization_tenants.equestrian_id"]),
    CheckConstraint("revision > 0", name="ck_normalization_revision_positive"),
    CheckConstraint(
        "status IN ('pending', 'scanning', 'awaiting_result', 'awaiting_cleanup', 'deleting', 'blocked', 'finalized')",
        name="ck_normalization_revision_status",
    ),
    CheckConstraint("part_count IS NULL OR part_count >= 0", name="ck_normalization_revision_parts"),
    CheckConstraint(
        "(cleanup_operation_id IS NULL AND cleanup_seq IS NULL) OR "
        "(cleanup_operation_id IS NOT NULL AND cleanup_seq IS NOT NULL AND cleanup_seq > 0)",
        name="ck_normalization_revision_cleanup",
    ),
    Index(
        "ux_normalization_active_tenant",
        "equestrian_id",
        unique=True,
        postgresql_where=text("status NOT IN ('pending', 'finalized')"),
    ),
    Index("ix_normalization_revision_ready", "status", "equestrian_id", "revision"),
)
normalization_snapshots = Table(
    "normalization_snapshots",
    metadata,
    tenant(),
    operation(),
    Column("query_params", ARRAY(Text), nullable=False),
    Column("hashes", ARRAY(Text), nullable=False),
    UniqueConstraint("equestrian_id", "operation_id", name="uq_normalization_snapshot"),
    revision_fk(),
)
normalization_scans = Table(
    "normalization_scans",
    metadata,
    tenant(),
    operation(),
    Column("cursor", BigInteger, nullable=False, server_default="0"),
    Column("high_water", BigInteger),
    Column("delete_cursor", BigInteger, nullable=False, server_default="0"),
    Column("next_part_index", Integer, nullable=False, server_default="0"),
    Column("sealed", Integer, nullable=False, server_default="0"),
    Column("lease_token", PG_UUID(as_uuid=True)),
    Column("lease_until", DateTime(timezone=True)),
    Column("attempts", Integer, nullable=False, server_default="0"),
    UniqueConstraint("equestrian_id", "operation_id", name="uq_normalization_scan"),
    revision_fk(),
    CheckConstraint(
        "cursor >= 0 AND (high_water IS NULL OR high_water >= cursor) AND delete_cursor >= 0 "
        "AND next_part_index >= 0 AND attempts >= 0 AND sealed IN (0, 1)",
        name="ck_normalization_scan_bounds",
    ),
    CheckConstraint(
        "sealed = 0 OR (high_water IS NOT NULL AND high_water = cursor)", name="ck_normalization_scan_sealed"
    ),
    CheckConstraint(
        "(lease_token IS NULL) = (lease_until IS NULL)",
        name="ck_normalization_scan_lease",
    ),
    Index("ix_normalization_scan_lease", "lease_until", "equestrian_id"),
)
normalization_mappings = Table(
    "normalization_mappings",
    metadata,
    tenant(),
    operation(),
    Column("source_id", PG_UUID(as_uuid=True), nullable=False),
    Column("target_id", PG_UUID(as_uuid=True), nullable=False),
    Column("source_scan_seq", BigInteger, nullable=False),
    Column("part_index", Integer, nullable=False),
    UniqueConstraint("equestrian_id", "operation_id", "source_id", name="uq_normalization_mapping_source"),
    UniqueConstraint("equestrian_id", "operation_id", "source_scan_seq", name="uq_normalization_mapping_seq"),
    revision_fk(),
    CheckConstraint("source_id <> target_id", name="ck_normalization_mapping_distinct"),
    CheckConstraint("source_scan_seq > 0 AND part_index >= 0", name="ck_normalization_mapping_bounds"),
    Index("ix_normalization_mapping_part", "equestrian_id", "operation_id", "part_index"),
)
normalization_redirects = Table(
    "normalization_redirects",
    metadata,
    tenant(),
    operation(),
    Column("source_id", PG_UUID(as_uuid=True), nullable=False),
    Column("target_id", PG_UUID(as_uuid=True), nullable=False),
    UniqueConstraint("equestrian_id", "source_id", name="uq_normalization_redirect_source"),
    revision_fk(),
    CheckConstraint("source_id <> target_id", name="ck_normalization_redirect_distinct"),
    Index("ix_normalization_redirect_target", "equestrian_id", "target_id"),
)
normalization_inbox = Table(
    "normalization_inbox",
    metadata,
    tenant(),
    Column("consumer", String(64), nullable=False),
    Column("message_id", PG_UUID(as_uuid=True), nullable=False),
    Column("operation_id", PG_UUID(as_uuid=True), nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("received_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("processed_at", DateTime(timezone=True)),
    UniqueConstraint("consumer", "message_id", name="uq_normalization_inbox_message"),
    Index("ix_normalization_inbox_operation", "equestrian_id", "operation_id", "processed_at"),
)
normalization_outbox = Table(
    "normalization_outbox",
    metadata,
    tenant(),
    operation(),
    Column("message_id", PG_UUID(as_uuid=True), primary_key=True),
    Column("kind", String(32), nullable=False),
    Column("part_index", Integer),
    Column("payload", JSONB, nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("published_at", DateTime(timezone=True)),
    Column("lease_token", PG_UUID(as_uuid=True)),
    Column("lease_until", DateTime(timezone=True)),
    Column("last_error_code", String(64)),
    revision_fk(),
    CheckConstraint(
        "attempts >= 0 AND (part_index IS NULL OR part_index >= 0)", name="ck_normalization_outbox_bounds"
    ),
    CheckConstraint("(lease_token IS NULL) = (lease_until IS NULL)", name="ck_normalization_outbox_lease"),
    Index(
        "ix_normalization_outbox_ready",
        "next_attempt_at",
        "lease_until",
        postgresql_where=text("published_at IS NULL"),
    ),
    Index(
        "ux_normalization_outbox_part",
        "equestrian_id",
        "operation_id",
        "kind",
        "part_index",
        unique=True,
        postgresql_where=text("part_index IS NOT NULL"),
    ),
    Index(
        "ux_normalization_outbox_terminal",
        "equestrian_id",
        "operation_id",
        "kind",
        unique=True,
        postgresql_where=text("part_index IS NULL"),
    ),
)
