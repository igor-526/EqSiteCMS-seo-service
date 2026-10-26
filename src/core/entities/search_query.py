from uuid import UUID

from .base import Entity, TimestampMixin


class SearchQuery(Entity, TimestampMixin):
    equestrian_id: UUID
    query: str
