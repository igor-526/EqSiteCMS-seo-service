from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from core.entities.parse_dispatch import ParseDispatchTrigger


@dataclass(frozen=True)
class ParseTarget:
    equestrian_id: UUID


class ParseProviderProtocol(Protocol):
    @property
    def code(self) -> str: ...

    @property
    def subject(self) -> str: ...

    async def list_targets(self, *, offset: int, limit: int) -> tuple[list[ParseTarget], int]: ...

    async def is_target(self, *, equestrian_id: UUID) -> bool: ...

    def build_params(self, *, trigger: ParseDispatchTrigger) -> dict[str, object]: ...
