from sqlalchemy import Column, Index, Table, Text
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from utils.basemodel import metadata, timestamp_columns, uuid_pk

pages = Table(
    "pages",
    metadata,
    uuid_pk(),
    *timestamp_columns(),
    Column("equestrian_id", PG_UUID(as_uuid=True), nullable=False),
    Column("page", Text, nullable=False),
    Column("page_hash", BYTEA, nullable=False),
    Index("ux_pages_equestrian_id_page_hash", "equestrian_id", "page_hash", unique=True),
    Index("ix_pages_page_trgm", "page", postgresql_using="gin", postgresql_ops={"page": "gin_trgm_ops"}),
)
Index(
    "ix_pages_equestrian_id_created_at",
    pages.c.equestrian_id,
    pages.c.created_at.desc(),
    pages.c.id.desc(),
)
