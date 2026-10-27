from sqlalchemy import CheckConstraint, Column, Date, DateTime, Index, Integer, String, Table, func, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from utils.basemodel import metadata

parse_dispatches = Table(
    "parse_dispatches",
    metadata,
    Column("id", PG_UUID(as_uuid=True), primary_key=True),
    Column("provider", String(32), nullable=False),
    Column("equestrian_id", PG_UUID(as_uuid=True), nullable=False),
    Column("trigger", String(16), nullable=False),
    Column("schedule_date", Date(), nullable=True),
    Column("status", String(16), nullable=False),
    Column("attempts", Integer(), nullable=False),
    Column("last_error_code", String(32), nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
    CheckConstraint("trigger IN ('schedule', 'manual')", name="ck_parse_dispatches_trigger"),
    CheckConstraint("status IN ('pending', 'published', 'failed')", name="ck_parse_dispatches_status"),
    CheckConstraint("attempts > 0", name="ck_parse_dispatches_attempts"),
    CheckConstraint(
        "(trigger = 'schedule' AND schedule_date IS NOT NULL) OR (trigger = 'manual' AND schedule_date IS NULL)",
        name="ck_parse_dispatches_schedule_date",
    ),
    Index(
        "ux_parse_dispatches_schedule",
        "provider",
        "equestrian_id",
        "schedule_date",
        unique=True,
        postgresql_where=text("trigger = 'schedule'"),
    ),
)
