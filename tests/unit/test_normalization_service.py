"""S-2a request-side UT-N-01/02/05/07; transport belongs to S-3."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities import BlacklistedHash, BlacklistedQueryParam
from core.entities.normalization import NormalizationSnapshot
from core.exceptions import AlreadyExistsError, ClientError, NotFoundError
from core.schemas.api import BlacklistItemInDto, SeoResolveInDto
from core.services import BlacklistedHashService, BlacklistedQueryParamService, PageNormalizer, PageService
from core.services.normalization_service import NormalizationService


def dependencies():
    storage, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = frozenset({"z", "utm"})
    hashes.get_values.return_value = frozenset({"top"})
    storage.resolve_redirect.side_effect = lambda **kwargs: kwargs["page_id"]
    coordinator = NormalizationService(storage, params, hashes)
    return storage, params, hashes, coordinator


@pytest.mark.parametrize("kind", ["query", "hash"])
async def test_normalization_post_locks_before_insert_then_snapshots_both_lists(kind):
    storage, params, hashes, coordinator = dependencies()
    tenant = uuid4()
    repo = params if kind == "query" else hashes
    entity_type = BlacklistedQueryParam if kind == "query" else BlacklistedHash
    cls = BlacklistedQueryParamService if kind == "query" else BlacklistedHashService
    repo.get_by_value.return_value = None
    saved = entity_type(equestrian_id=tenant, value="new")
    events = []

    async def lock(**kwargs):
        assert kwargs == {"equestrian_id": tenant}
        events.append("lock")

    async def insert(entity, **kwargs):
        assert events == ["lock"]
        assert kwargs == {"equestrian_id": tenant}
        events.append("insert")
        repo.get_values.return_value = repo.get_values.return_value | {entity.value}
        return saved

    async def revision(**kwargs):
        assert events == ["lock", "insert"]
        assert kwargs["equestrian_id"] == tenant
        assert kwargs["operation_id"] is not None
        assert kwargs["snapshot"] == NormalizationSnapshot(
            tuple(sorted(params.get_values.return_value)), tuple(sorted(hashes.get_values.return_value))
        )
        events.append("revision+snapshot+scan")

    storage.lock_tenant.side_effect = lock
    repo.create.side_effect = insert
    storage.create_revision.side_effect = revision
    result = await cls(repo, normalization_service=coordinator).create(
        equestrian_id=tenant, data=BlacklistItemInDto(value=" new ")
    )
    assert result.id == saved.id and result.value == "new"
    assert set(result.model_dump()) == {"id", "value", "created_at"}
    assert events == ["lock", "insert", "revision+snapshot+scan"]
    storage.activate_next.assert_not_awaited()
    storage.enqueue_outbox.assert_not_awaited()


@pytest.mark.parametrize("kind", ["query", "hash"])
@pytest.mark.parametrize("failure", ["invalid", "duplicate", "insert"])
async def test_normalization_rejected_post_never_plans(kind, failure):
    storage, params, hashes, coordinator = dependencies()
    tenant = uuid4()
    repo = params if kind == "query" else hashes
    cls = BlacklistedQueryParamService if kind == "query" else BlacklistedHashService
    repo.get_by_value.return_value = None
    if failure == "duplicate":
        repo.get_by_value.return_value = object()
    if failure == "insert":
        repo.create.side_effect = AlreadyExistsError()
    with pytest.raises(ClientError):
        await cls(repo, normalization_service=coordinator).create(
            equestrian_id=tenant, data=BlacklistItemInDto(value="a b" if failure == "invalid" else "new")
        )
    storage.create_revision.assert_not_awaited()
    params.get_values.assert_not_awaited()
    hashes.get_values.assert_not_awaited()
    if failure == "invalid":
        storage.lock_tenant.assert_not_awaited()


@pytest.mark.parametrize("kind", ["query", "hash"])
async def test_normalization_patch_delete_lock_but_do_not_plan_or_split(kind):
    storage, params, hashes, coordinator = dependencies()
    tenant = uuid4()
    repo = params if kind == "query" else hashes
    cls = BlacklistedQueryParamService if kind == "query" else BlacklistedHashService
    entity_type = BlacklistedQueryParam if kind == "query" else BlacklistedHash
    entity = entity_type(equestrian_id=tenant, value="old")
    repo.get_by_id.return_value = entity
    repo.get_by_value.return_value = None
    repo.update.return_value = entity.model_copy(update={"value": "new"})
    service = cls(repo, normalization_service=coordinator)
    updated = await service.update(equestrian_id=tenant, item_id=entity.id, data=BlacklistItemInDto(value="new"))
    assert updated.id == entity.id and updated.value == "new"
    await service.delete(equestrian_id=tenant, item_id=entity.id)
    repo.get_by_id.return_value = None
    with pytest.raises(NotFoundError):
        await service.delete(equestrian_id=tenant, item_id=entity.id)
    assert storage.lock_tenant.await_count == 3
    storage.create_revision.assert_not_awaited()
    repo.delete.assert_awaited_once_with(entity.id, equestrian_id=tenant)


async def test_normalization_resolve_locks_before_read_insert_and_follows_current_uuid_alias():
    storage, params, hashes, coordinator = dependencies()
    tenant, source, target = uuid4(), uuid4(), uuid4()
    pages = AsyncMock()
    events = []

    async def lock(**kwargs):
        events.append("lock")

    async def read(**kwargs):
        assert events == ["lock"]
        return frozenset({"utm"})

    async def insert(**kwargs):
        assert events == ["lock"]
        assert kwargs == {"equestrian_id": tenant, "values": ["/a"]}
        events.append("insert")
        return {"/a": source}

    async def redirect(**kwargs):
        assert events == ["lock", "insert"]
        assert kwargs == {"equestrian_id": tenant, "page_id": source}
        return target

    storage.lock_tenant.side_effect = lock
    params.get_values.side_effect = read
    pages.resolve.side_effect = insert
    storage.resolve_redirect.side_effect = redirect
    service = PageService(pages, PageNormalizer(), params, hashes, normalization_service=coordinator)
    result = await service.resolve(equestrian_id=tenant, data=SeoResolveInDto(items=["/a?utm=1#top", "/a"]))
    assert [item.id for item in result.items] == [target, target]
    assert [item.normalized for item in result.items] == ["/a", "/a"]
    storage.resolve_redirect.assert_awaited_once()
    storage.create_revision.assert_not_awaited()


async def test_normalization_resolve_reloads_current_rules_not_backfill_snapshot():
    storage, params, hashes, coordinator = dependencies()
    pages = AsyncMock()
    tenant, old, current = uuid4(), uuid4(), uuid4()
    params.get_values.side_effect = [frozenset({"utm"}), frozenset()]
    hashes.get_values.return_value = frozenset()
    pages.resolve.side_effect = [{"/a": old}, {"/a?utm=1": current}]
    service = PageService(pages, PageNormalizer(), params, hashes, normalization_service=coordinator)
    data = SeoResolveInDto(items=["/a?utm=1"])
    assert (await service.resolve(equestrian_id=tenant, data=data)).items[0].id == old
    assert (await service.resolve(equestrian_id=tenant, data=data)).items[0].id == current
    assert storage.lock_tenant.await_count == 2
    storage.get_revision.assert_not_awaited()


async def test_normalization_di_uses_one_context_local_session():
    from dependency_injector import providers

    from containers.repositories import RepositoriesContainer
    from containers.services import ServicesContainer
    from repositories.blacklisted_hash_repository import BlacklistedHashRepository
    from repositories.blacklisted_query_param_repository import BlacklistedQueryParamRepository
    from repositories.normalization_repository import NormalizationRepository
    from repositories.page_repository import PageRepository

    session = object()
    repositories = RepositoriesContainer()
    services = ServicesContainer(repositories_container=repositories)
    with repositories.session.override(providers.Object(session)):
        coordinator = services.normalization_service()
        page = services.page_service()
        query = services.blacklisted_query_param_service()
        hashes = services.blacklisted_hash_service()
        assert isinstance(coordinator._repository, NormalizationRepository)
        assert coordinator._repository.session is session
        assert isinstance(coordinator._params, BlacklistedQueryParamRepository)
        assert coordinator._params.session is session
        assert isinstance(coordinator._hashes, BlacklistedHashRepository)
        assert coordinator._hashes.session is session
        assert isinstance(page._page_repository, PageRepository)
        assert page._page_repository.session is session
        assert isinstance(query._blacklisted_query_param_repository, BlacklistedQueryParamRepository)
        assert query._blacklisted_query_param_repository.session is session
        assert isinstance(hashes._blacklisted_hash_repository, BlacklistedHashRepository)
        assert hashes._blacklisted_hash_repository.session is session
        for service in (page, query, hashes):
            repository = service._normalization._repository
            assert isinstance(repository, NormalizationRepository)
            assert repository.session is session
