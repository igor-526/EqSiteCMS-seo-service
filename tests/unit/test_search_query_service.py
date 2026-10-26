from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.exceptions import ClientError
from core.schemas.api import SeoResolveInDto
from core.services import SearchQueryNormalizer, SearchQueryService


async def test_resolve_preserves_order_deduplicates_and_is_idempotent() -> None:
    tenant, horse_id, stable_id = uuid4(), uuid4(), uuid4()
    repository = AsyncMock()
    repository.resolve.return_value = {"Лошадь": horse_id, "Денник": stable_id}
    service = SearchQueryService(repository, SearchQueryNormalizer())
    data = SeoResolveInDto(items=["лошадь", "ДЕННИК", "ЛОШАДЬ", " лошадь "])
    first = await service.resolve(equestrian_id=tenant, data=data)
    second = await service.resolve(equestrian_id=tenant, data=data)
    assert first == second
    assert [item.source for item in first.items] == data.items
    assert [item.id for item in first.items] == [horse_id, stable_id, horse_id, horse_id]
    assert [item.normalized for item in first.items] == ["Лошадь", "Денник", "Лошадь", "Лошадь"]
    assert repository.resolve.await_count == 2
    repository.resolve.assert_awaited_with(equestrian_id=tenant, values=["Лошадь", "Денник"])


async def test_invalid_middle_item_does_not_write_or_expose_source() -> None:
    repository = AsyncMock()
    service = SearchQueryService(repository, SearchQueryNormalizer())
    with pytest.raises(ClientError, match=r"items\[1\]") as error:
        await service.resolve(equestrian_id=uuid4(), data=SeoResolveInDto(items=["лошадь", " \t ", "денник"]))
    assert str(error.value) == "Некорректное значение элемента items[1]"
    repository.resolve.assert_not_awaited()


async def test_resolve_max_batch() -> None:
    repository = AsyncMock()
    values = [f"Запрос {index}" for index in range(1000)]
    repository.resolve.return_value = {value: uuid4() for value in values}
    result = await SearchQueryService(repository, SearchQueryNormalizer()).resolve(
        equestrian_id=uuid4(), data=SeoResolveInDto(items=values)
    )
    assert len(result.items) == 1000
    assert len(repository.resolve.call_args.kwargs["values"]) == 1000
