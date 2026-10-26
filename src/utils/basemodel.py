import uuid

from sqlalchemy import Column, DateTime, MetaData, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

metadata = MetaData()


def uuid_pk() -> Column:
    """Primary key UUID column."""
    return Column("id", PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


def timestamp_columns() -> tuple[Column, Column]:
    """Created and updated timestamp columns."""
    return (
        Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
    )
