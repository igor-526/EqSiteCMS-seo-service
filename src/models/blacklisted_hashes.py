from sqlalchemy import Column, Index, String, Table
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from utils.basemodel import metadata, timestamp_columns, uuid_pk

blacklisted_hashes = Table(
    "blacklisted_hashes",
    metadata,
    uuid_pk(),
    *timestamp_columns(),
    Column("equestrian_id", PG_UUID(as_uuid=True), nullable=False),
    Column("value", String(255), nullable=False),
    Index("ux_blacklisted_hashes_equestrian_id_value", "equestrian_id", "value", unique=True),
)
