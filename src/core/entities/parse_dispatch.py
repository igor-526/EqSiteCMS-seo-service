from datetime import date, datetime
from enum import StrEnum
from uuid import UUID

from .base import Entity


class ParseDispatchStatus(StrEnum):
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"


class ParseDispatchTrigger(StrEnum):
    SCHEDULE = "schedule"
    MANUAL = "manual"


class ParseDispatch(Entity):
    provider: str
    equestrian_id: UUID
    trigger: ParseDispatchTrigger
    schedule_date: date | None
    status: ParseDispatchStatus
    attempts: int
    last_error_code: str | None
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None
