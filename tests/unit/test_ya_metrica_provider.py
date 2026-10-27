from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest

from clients.yandex_service import YandexServiceClient
from core.entities.parse_dispatch import ParseDispatchTrigger
from core.exceptions import ParseTargetsUnavailableError
from core.protocols.parsing.provider import ParseProviderProtocol
from core.schemas.clients.yandex_service import MetricaBinding, MetricaBindingList
from providers.ya_metrica import YaMetricaParseProvider


async def test_paginated_targets() -> None:
    ids = sorted([uuid4() for _ in range(250)])
    client = AsyncMock()
    client.list_metrica_bindings.side_effect = [
        MetricaBindingList(
            total=250, items=[MetricaBinding(equestrian_id=id, counter_id="123") for id in ids[i : i + 100]]
        )
        for i in (0, 100, 200)
    ]
    provider: ParseProviderProtocol = YaMetricaParseProvider(client=client)
    actual: list[UUID] = []
    for offset in (0, 100, 200):
        items, total = await provider.list_targets(offset=offset, limit=100)
        assert total == 250
        actual.extend(item.equestrian_id for item in items)
    assert actual == ids
    assert [call.kwargs for call in client.list_metrica_bindings.await_args_list] == [
        {"offset": i, "limit": 100} for i in (0, 100, 200)
    ]


@pytest.mark.parametrize("exists", [True, False])
async def test_target_preflight(exists: bool) -> None:
    id = uuid4()
    client = AsyncMock()
    client.get_metrica_binding.return_value = MetricaBinding(equestrian_id=id, counter_id="1") if exists else None
    provider = YaMetricaParseProvider(client=client)
    assert await provider.is_target(equestrian_id=id) is exists
    client.get_metrica_binding.assert_awaited_once_with(equestrian_id=id)
    for trigger in ParseDispatchTrigger:
        assert provider.build_params(trigger=trigger) == {}
    params = provider.build_params(trigger=ParseDispatchTrigger.MANUAL)
    params["changed"] = True
    assert provider.build_params(trigger=ParseDispatchTrigger.MANUAL) == {}


@pytest.mark.parametrize("payload", [None, {"total": 1, "items": [{"equestrian_id": "invalid"}]}])
@pytest.mark.parametrize("method", ["list", "target"])
async def test_upstream_failure_is_not_a_missing_target(payload, method: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if payload is None:
            raise httpx.ConnectError("private", request=request)
        return httpx.Response(200, json=payload)

    provider = YaMetricaParseProvider(
        client=YandexServiceClient(base_url="http://private-peer", transport=httpx.MockTransport(handler))
    )
    with pytest.raises(ParseTargetsUnavailableError):
        if method == "list":
            await provider.list_targets(offset=0, limit=100)
        else:
            await provider.is_target(equestrian_id=uuid4())
