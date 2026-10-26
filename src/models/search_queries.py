from sqlalchemy import Column, Index, Table, Text
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from utils.basemodel import metadata, timestamp_columns, uuid_pk

search_queries = Table(
    "search_queries",
    metadata,
    uuid_pk(),
    *timestamp_columns(),
    Column("equestrian_id", PG_UUID(as_uuid=True), nullable=False),
    Column("query", Text, nullable=False),
    Column("query_hash", BYTEA, nullable=False),
    Index("ux_search_queries_equestrian_id_query_hash", "equestrian_id", "query_hash", unique=True),
    Index("ix_search_queries_query_trgm", "query", postgresql_using="gin", postgresql_ops={"query": "gin_trgm_ops"}),
)
Index(
    "ix_search_queries_equestrian_id_created_at",
    search_queries.c.equestrian_id,
    search_queries.c.created_at.desc(),
    search_queries.c.id.desc(),
)
