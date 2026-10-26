from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.exceptions import ClientError
from core.schemas.api import SeoResolveInDto
from core.services import PageNormalizer, PageService


async def test_resolve_loads_both_tenant_blacklists_once_and_preserves_order() -> None:
    tenant, first_id, second_id = uuid4(), uuid4(), uuid4()
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = frozenset({"utm_source"})
    hashes.get_values.return_value = frozenset({"top"})
    pages.resolve.return_value = {"/a": first_id, "/b#keep": second_id}
    service = PageService(pages, PageNormalizer(), params, hashes)
    data = SeoResolveInDto(items=["https://site.ru/a?utm_source=ya#top", "/b#keep", "/a"])
    result = await service.resolve(equestrian_id=tenant, data=data)
    params.get_values.assert_awaited_once_with(equestrian_id=tenant)
    hashes.get_values.assert_awaited_once_with(equestrian_id=tenant)
    pages.resolve.assert_awaited_once_with(equestrian_id=tenant, values=["/a", "/b#keep"])
    assert [item.source for item in result.items] == data.items
    assert [item.normalized for item in result.items] == ["/a", "/b#keep", "/a"]
    assert [item.id for item in result.items] == [first_id, second_id, first_id]


async def test_invalid_middle_page_never_writes() -> None:
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = hashes.get_values.return_value = frozenset()
    with pytest.raises(ClientError) as error:
        await PageService(pages, PageNormalizer(), params, hashes).resolve(
            equestrian_id=uuid4(), data=SeoResolveInDto(items=["/a", "ftp://private.invalid/secret", "/b"])
        )
    assert str(error.value) == "Некорректное значение элемента items[1]"
    pages.resolve.assert_not_awaited()


async def test_blacklists_are_reloaded_for_each_tenant_batch() -> None:
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.side_effect = [frozenset({"track"}), frozenset()]
    hashes.get_values.return_value = frozenset()
    pages.resolve.return_value = {"/a": uuid4(), "/a?track=x": uuid4()}
    service = PageService(pages, PageNormalizer(), params, hashes)
    first_tenant, second_tenant = uuid4(), uuid4()
    data = SeoResolveInDto(items=["/a?track=x"])
    first = await service.resolve(equestrian_id=first_tenant, data=data)
    second = await service.resolve(equestrian_id=second_tenant, data=data)
    assert first.items[0].normalized == "/a"
    assert second.items[0].normalized == "/a?track=x"
    assert params.get_values.await_count == hashes.get_values.await_count == 2
    assert params.get_values.await_args_list[0].kwargs == {"equestrian_id": first_tenant}
    assert params.get_values.await_args_list[1].kwargs == {"equestrian_id": second_tenant}


async def test_max_page_batch() -> None:
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = hashes.get_values.return_value = frozenset()
    values = [f"/page/{index}" for index in range(1000)]
    pages.resolve.return_value = {value: uuid4() for value in values}
    result = await PageService(pages, PageNormalizer(), params, hashes).resolve(
        equestrian_id=uuid4(), data=SeoResolveInDto(items=values)
    )
    assert len(result.items) == 1000
    assert params.get_values.await_count == hashes.get_values.await_count == 1
