from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from core.schemas.messaging import ParseCommand


class PublishAckProtocol(Protocol):
    @property
    def duplicate(self) -> bool | None: ...


class ParseCommandTransportProtocol(Protocol):
    async def publish(
        self, *, subject: str, payload: bytes, headers: dict[str, str] | None = None
    ) -> PublishAckProtocol: ...


@dataclass(frozen=True)
class PublishedParseCommand:
    job_id: UUID
    message_id: UUID
    duplicate: bool


class ParseCommandPublisherProtocol(Protocol):
    async def publish(self, *, command: ParseCommand) -> PublishedParseCommand: ...
