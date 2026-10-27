from uuid import UUID

from core.entities.parse_dispatch import ParseDispatchTrigger
from core.protocols.clients.yandex_service import YandexServiceClientProtocol
from core.protocols.parsing.provider import ParseTarget


class YaMetricaParseProvider:
    code = "ya_metrica"
    subject = "commands.parse.ya_metrica.run"

    def __init__(self, *, client: YandexServiceClientProtocol) -> None:
        self._client = client

    async def list_targets(self, *, offset: int, limit: int) -> tuple[list[ParseTarget], int]:
        page = await self._client.list_metrica_bindings(offset=offset, limit=limit)
        return [ParseTarget(equestrian_id=item.equestrian_id) for item in page.items], page.total

    async def is_target(self, *, equestrian_id: UUID) -> bool:
        return await self._client.get_metrica_binding(equestrian_id=equestrian_id) is not None

    def build_params(self, *, trigger: ParseDispatchTrigger) -> dict[str, object]:
        return {}
