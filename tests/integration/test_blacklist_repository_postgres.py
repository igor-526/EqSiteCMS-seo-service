"""IT-SEO-06: tenant-scoped settings, constraints, races and no retroactivity."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.entities import BlacklistedHash, BlacklistedQueryParam
from core.exceptions import AlreadyExistsError, NotFoundError, RepositoryError
from repositories import BlacklistedHashRepository, BlacklistedQueryParamRepository, PageRepository

pytestmark = pytest.mark.infrastructure
REPOSITORIES = [(BlacklistedHashRepository, BlacklistedHash), (BlacklistedQueryParamRepository, BlacklistedQueryParam)]


@pytest.mark.parametrize(("repository_cls", "entity_cls"), REPOSITORIES)
async def test_crud_isolation_sorting_and_pages_unchanged(pg_session: AsyncSession, repository_cls, entity_cls):
    repository = repository_cls(pg_session)
    tenant, other = uuid4(), uuid4()
    pages = PageRepository(pg_session)
    await pages.resolve(equestrian_id=tenant, values=["/a?utm_source=x#top"])
    before = (await pg_session.execute(select(pages.table))).mappings().all()
    assert await repository.get_all(equestrian_id=tenant) == []
    assert await repository.get_values(equestrian_id=tenant) == frozenset()
    first = await repository.create(entity_cls(equestrian_id=tenant, value="z"), equestrian_id=tenant)
    # Tenant from the method always wins over an injected entity tenant.
    second = await repository.create(entity_cls(equestrian_id=other, value="a"), equestrian_id=tenant)
    assert second.equestrian_id == tenant
    foreign = await repository.create(entity_cls(equestrian_id=other, value="z"), equestrian_id=other)
    assert foreign.id != first.id
    assert [item.value for item in await repository.get_all(equestrian_id=tenant)] == ["a", "z"]
    assert await repository.get_values(equestrian_id=tenant) == frozenset({"a", "z"})
    assert await repository.get_by_value("a", equestrian_id=other) is None
    assert await repository.get_by_value("z", equestrian_id=tenant) == first
    assert await repository.get_by_id(foreign.id, equestrian_id=tenant) is None
    assert await repository.get_by_id(uuid4(), equestrian_id=tenant) is None
    for id in [foreign.id, uuid4()]:
        with pytest.raises(NotFoundError):
            await repository.update(entity_cls(id=id, equestrian_id=tenant, value="changed"), equestrian_id=tenant)
        with pytest.raises(NotFoundError):
            await repository.delete(id, equestrian_id=tenant)
    assert await repository.get_by_id(foreign.id, equestrian_id=other) == foreign
    # A forged entity tenant must not move an owned row to another tenant.
    changed = await repository.update(
        first.model_copy(update={"value": "b", "equestrian_id": other}), equestrian_id=tenant
    )
    assert changed.id == first.id and changed.equestrian_id == tenant and changed.value == "b"
    assert changed.created_at == first.created_at and changed.updated_at is not None
    await repository.delete(first.id, equestrian_id=tenant)
    with pytest.raises(NotFoundError):
        await repository.delete(first.id, equestrian_id=tenant)
    assert await repository.get_values(equestrian_id=tenant) == frozenset({"a"})
    after = (await pg_session.execute(select(pages.table))).mappings().all()
    assert before == after


async def test_independent_lists(pg_session: AsyncSession):
    tenant = uuid4()
    param = await BlacklistedQueryParamRepository(pg_session).create(
        BlacklistedQueryParam(equestrian_id=tenant, value="top"), equestrian_id=tenant
    )
    hash_item = await BlacklistedHashRepository(pg_session).create(
        BlacklistedHash(equestrian_id=tenant, value="top"), equestrian_id=tenant
    )
    assert param.id != hash_item.id


@pytest.mark.parametrize(("repository_cls", "entity_cls"), REPOSITORIES)
async def test_constraint_error_mapping_and_rollback(pg_session: AsyncSession, repository_cls, entity_cls):
    repository = repository_cls(pg_session)
    tenant = uuid4()
    first = await repository.create(entity_cls(equestrian_id=tenant, value="sensitive"), equestrian_id=tenant)
    second = await repository.create(entity_cls(equestrian_id=tenant, value="other"), equestrian_id=tenant)
    await pg_session.commit()
    for operation in ["create", "update"]:
        with pytest.raises(AlreadyExistsError, match="^Элемент уже существует$"):
            if operation == "create":
                await repository.create(entity_cls(equestrian_id=tenant, value="sensitive"), equestrian_id=tenant)
            else:
                await repository.update(second.model_copy(update={"value": "sensitive"}), equestrian_id=tenant)
        await pg_session.rollback()
        assert await repository.get_by_id(second.id, equestrian_id=tenant) == second
    # A primary-key conflict is not a duplicate setting value, so remains 500.
    with pytest.raises(RepositoryError, match="^Внутренняя ошибка сервиса$"):
        await repository.create(first.model_copy(update={"value": "different"}), equestrian_id=tenant)
    await pg_session.rollback()
    # NOT NULL failures must not become misleading 409 responses either.
    with pytest.raises(RepositoryError, match="^Внутренняя ошибка сервиса$"):
        await repository.create(entity_cls.model_construct(equestrian_id=tenant, value=None), equestrian_id=tenant)
    await pg_session.rollback()
    # pg_trgm needs public during migrations, but a deliberately missing table must
    # never fall back to its production namesake. Limit only this transaction.
    schema = await pg_session.scalar(text("SELECT current_schema()"))
    assert isinstance(schema, str) and schema.startswith("test_seo_") and len(schema) == 41
    assert all(character in "0123456789abcdef" for character in schema.removeprefix("test_seo_"))
    await pg_session.execute(text("SELECT set_config('search_path', :schema, true)"), {"schema": schema})
    assert await pg_session.scalar(text("SHOW search_path")) == schema
    # Explicitly qualify the destructive statement with the validated test schema.
    await pg_session.execute(text(f'DROP TABLE "{schema}"."{repository.table.name}"'))
    assert await pg_session.scalar(text("SELECT to_regclass(:table)"), {"table": repository.table.name}) is None
    with pytest.raises(RepositoryError, match="^Внутренняя ошибка сервиса$"):
        await repository.get_values(equestrian_id=tenant)
    await pg_session.rollback()
    assert await repository.get_by_id(first.id, equestrian_id=tenant) == first


@pytest.mark.parametrize(("repository_cls", "entity_cls"), REPOSITORIES)
async def test_concurrent_duplicate_maps_to_already_exists(pg_engine: AsyncEngine, repository_cls, entity_cls):
    tenant = uuid4()
    barrier = asyncio.Barrier(2)

    async def create():
        try:
            async with AsyncSession(pg_engine) as session, session.begin():
                await session.execute(text("SET LOCAL lock_timeout = '5s'"))
                await barrier.wait()
                return await repository_cls(session).create(
                    entity_cls(equestrian_id=tenant, value="duplicate"), equestrian_id=tenant
                )
        except AlreadyExistsError as exc:
            return exc

    results = await asyncio.wait_for(asyncio.gather(create(), create()), timeout=15)
    assert sum(isinstance(result, AlreadyExistsError) for result in results) == 1
    async with AsyncSession(pg_engine) as session:
        assert len(await repository_cls(session).get_all(equestrian_id=tenant)) == 1
