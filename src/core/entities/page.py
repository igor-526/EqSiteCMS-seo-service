from uuid import UUID

from .base import Entity, TimestampMixin


class Page(Entity, TimestampMixin):
    equestrian_id: UUID
    page: str
