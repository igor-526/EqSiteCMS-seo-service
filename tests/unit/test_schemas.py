from uuid import uuid4

import pytest
from pydantic import ValidationError

from core.entities import BlacklistedHash, Page, SearchQuery
from core.schemas import Schema
from core.schemas.api import BlacklistItemInDto, BlacklistItemOutDto, PageOutDto, SearchQueryOutDto, SeoResolveInDto


@pytest.mark.parametrize("size", [1, 1000])
def test_resolve_size_boundaries(size: int) -> None:
    assert len(SeoResolveInDto(items=["x"] * size).items) == size


@pytest.mark.parametrize("items", [[], ["x"] * 1001, ["x", 1], [None], "x"])
def test_resolve_rejects_invalid_structure(items: object) -> None:
    with pytest.raises(ValidationError):
        SeoResolveInDto.model_validate({"items": items})


@pytest.mark.parametrize("data", [{"items": ["x"], "tenant": "x"}, {}, {"items": None}])
def test_resolve_forbids_unknown_or_missing_fields(data: object) -> None:
    with pytest.raises(ValidationError):
        SeoResolveInDto.model_validate(data)


@pytest.mark.parametrize("data", [{}, {"value": 1}, {"value": "x", "id": "x"}])
def test_blacklist_structure(data: object) -> None:
    with pytest.raises(ValidationError):
        BlacklistItemInDto.model_validate(data)


def test_business_validation_is_not_in_dto_and_strings_are_unbounded() -> None:
    assert BlacklistItemInDto(value="").value == ""
    assert len(SeoResolveInDto(items=["я" * 100000]).items[0]) == 100000


def test_outputs_hide_internal_fields() -> None:
    tenant = uuid4()
    cases: list[tuple[type[Schema], object, str]] = [
        (SearchQueryOutDto, SearchQuery(equestrian_id=tenant, query="X"), "query"),
        (PageOutDto, Page(equestrian_id=tenant, page="/"), "page"),
        (BlacklistItemOutDto, BlacklistedHash(equestrian_id=tenant, value="top"), "value"),
    ]
    for dto, entity, field in cases:
        assert set(dto.model_validate(entity).model_dump()) == {"id", field, "created_at"}
