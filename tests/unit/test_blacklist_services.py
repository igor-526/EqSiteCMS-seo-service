from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities import BlacklistedHash, BlacklistedQueryParam
from core.exceptions import AlreadyExistsError, ClientError, NotFoundError
from core.schemas.api import BlacklistItemInDto
from core.services import BlacklistedHashService, BlacklistedQueryParamService


@pytest.fixture(params=["param", "hash"])
def case(
    request: pytest.FixtureRequest,
) -> tuple[
    BlacklistedQueryParamService | BlacklistedHashService,
    AsyncMock,
    type[BlacklistedQueryParam] | type[BlacklistedHash],
]:
    repository = AsyncMock()
    repository.get_by_value.return_value = None
    if request.param == "param":
        return BlacklistedQueryParamService(repository), repository, BlacklistedQueryParam
    return BlacklistedHashService(repository), repository, BlacklistedHash


type Case = tuple[
    BlacklistedQueryParamService | BlacklistedHashService,
    AsyncMock,
    type[BlacklistedQueryParam] | type[BlacklistedHash],
]


async def test_create_normalizes_and_uses_repository_result(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    saved = entity_type(equestrian_id=tenant, value="top")
    repository.create.return_value = saved
    result = await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value=" top "))
    assert result.id == saved.id and result.value == "top"
    repository.get_by_value.assert_awaited_once_with("top", equestrian_id=tenant)
    entity = repository.create.call_args.args[0]
    assert entity.value == "top" and entity.equestrian_id == tenant
    assert repository.create.call_args.kwargs == {"equestrian_id": tenant}
    assert set(result.model_dump()) == {"id", "value", "created_at"}


@pytest.mark.parametrize("value", ["", " ", "a b", "a\tb", "a\x00b", "a#b", "x" * 256])
async def test_invalid_values_do_not_reach_repository(case: Case, value: str) -> None:
    service, repository, _ = case
    with pytest.raises(ClientError):
        await service.create(equestrian_id=uuid4(), data=BlacklistItemInDto(value=value))
    repository.get_by_value.assert_not_awaited()
    repository.create.assert_not_awaited()


async def test_max_length_and_case_preserved(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    value = "Я" * 255
    repository.create.return_value = entity_type(equestrian_id=tenant, value=value)
    result = await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value=value))
    assert result.value == value


async def test_duplicate_create_and_race_are_conflicts(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    repository.get_by_value.return_value = entity_type(equestrian_id=tenant, value="top")
    with pytest.raises(AlreadyExistsError):
        await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value="top"))
    repository.create.assert_not_awaited()
    repository.get_by_value.return_value = None
    repository.create.side_effect = AlreadyExistsError()
    with pytest.raises(AlreadyExistsError):
        await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value="top"))


async def test_update_preserves_identity_and_returns_saved_entity(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    old = entity_type(equestrian_id=tenant, value="old")
    saved = old.model_copy(update={"value": "new"})
    repository.get_by_id.return_value = old
    repository.update.return_value = saved
    result = await service.update(equestrian_id=tenant, item_id=old.id, data=BlacklistItemInDto(value=" new "))
    assert result.id == old.id and result.value == "new"
    updated = repository.update.call_args.args[0]
    assert updated.value == "new" and updated.updated_at is not None
    assert updated.id == old.id and updated.created_at == old.created_at and updated.equestrian_id == tenant
    assert old.value == "old" and old.updated_at is None
    repository.get_by_id.assert_awaited_once_with(old.id, equestrian_id=tenant)
    assert repository.update.call_args.kwargs == {"equestrian_id": tenant}


async def test_unchanged_update_allowed_but_other_duplicate_rejected(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    old = entity_type(equestrian_id=tenant, value="top")
    repository.get_by_id.return_value = repository.get_by_value.return_value = old
    repository.update.return_value = old
    await service.update(equestrian_id=tenant, item_id=old.id, data=BlacklistItemInDto(value="top"))
    repository.update.assert_awaited_once()
    repository.update.reset_mock()
    repository.get_by_value.return_value = entity_type(equestrian_id=tenant, value="other")
    with pytest.raises(AlreadyExistsError):
        await service.update(equestrian_id=tenant, item_id=old.id, data=BlacklistItemInDto(value="other"))
    repository.update.assert_not_awaited()


async def test_invalid_update_and_concurrent_duplicate(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    entity = entity_type(equestrian_id=tenant, value="old")
    repository.get_by_id.return_value = entity
    with pytest.raises(ClientError):
        await service.update(equestrian_id=tenant, item_id=entity.id, data=BlacklistItemInDto(value="a b"))
    repository.update.assert_not_awaited()
    repository.get_by_value.assert_not_awaited()
    repository.update.side_effect = AlreadyExistsError()
    with pytest.raises(AlreadyExistsError):
        await service.update(equestrian_id=tenant, item_id=entity.id, data=BlacklistItemInDto(value="new"))
    assert entity.value == "old"


@pytest.mark.parametrize("operation", ["update", "delete"])
async def test_missing_or_foreign_item_uses_scoped_lookup(case: Case, operation: str) -> None:
    service, repository, _ = case
    tenant, item_id = uuid4(), uuid4()
    repository.get_by_id.return_value = None
    with pytest.raises(NotFoundError):
        if operation == "update":
            await service.update(equestrian_id=tenant, item_id=item_id, data=BlacklistItemInDto(value="x"))
        else:
            await service.delete(equestrian_id=tenant, item_id=item_id)
    repository.get_by_id.assert_awaited_once_with(item_id, equestrian_id=tenant)
    repository.update.assert_not_awaited()
    repository.delete.assert_not_awaited()


async def test_delete_and_list_only_touch_blacklist(case: Case) -> None:
    service, repository, entity_type = case
    tenant = uuid4()
    entities = [entity_type(equestrian_id=tenant, value=value) for value in ["a", "z"]]
    repository.get_all.return_value = entities
    result = await service.list_all(equestrian_id=tenant)
    assert result.total == 2 and [item.value for item in result.items] == ["a", "z"]
    repository.get_all.assert_awaited_once_with(equestrian_id=tenant)
    repository.get_by_id.return_value = entities[0]
    await service.delete(equestrian_id=tenant, item_id=entities[0].id)
    repository.delete.assert_awaited_once_with(entities[0].id, equestrian_id=tenant)
    assert {call[0] for call in repository.mock_calls} == {"get_all", "get_by_id", "delete"}


@pytest.mark.parametrize("value", ["#", "##top", "# top"])
async def test_invalid_hash_prefix(value: str) -> None:
    repository = AsyncMock()
    with pytest.raises(ClientError):
        await BlacklistedHashService(repository).create(equestrian_id=uuid4(), data=BlacklistItemInDto(value=value))
    repository.create.assert_not_awaited()


async def test_hash_strips_one_prefix_and_allows_url_symbols() -> None:
    tenant = uuid4()
    repository = AsyncMock()
    repository.get_by_value.return_value = None
    repository.create.return_value = BlacklistedHash(equestrian_id=tenant, value="!/catalog?x=1")
    await BlacklistedHashService(repository).create(
        equestrian_id=tenant, data=BlacklistItemInDto(value=" #!/catalog?x=1 ")
    )
    repository.get_by_value.assert_awaited_once_with("!/catalog?x=1", equestrian_id=tenant)


@pytest.mark.parametrize("value", ["a&b", "a=b", "a?b", "#top"])
async def test_param_forbids_url_symbols(value: str) -> None:
    repository = AsyncMock()
    with pytest.raises(ClientError):
        await BlacklistedQueryParamService(repository).create(
            equestrian_id=uuid4(), data=BlacklistItemInDto(value=value)
        )
    repository.create.assert_not_awaited()
