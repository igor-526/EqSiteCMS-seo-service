"""IT-SEO-03..05: real PostgreSQL, isolated schema from integration fixtures."""

import asyncio
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.exceptions import ClientError, RepositoryError
from repositories import PageRepository, SearchQueryRepository

pytestmark = pytest.mark.infrastructure
REPOSITORIES = [SearchQueryRepository, PageRepository]


@pytest.mark.parametrize("repository_cls", REPOSITORIES)
async def test_resolve_idempotency_tenants_long_values(pg_session: AsyncSession, repository_cls):
    """IT-SEO-03: mixed batch, 1000 items, long text, SHA-256 and immutable timestamps."""
    repository = repository_cls(pg_session)
    tenant, other = uuid4(), uuid4()
    assert await repository.resolve(equestrian_id=tenant, values=[]) == {}
    first = await repository.resolve(equestrian_id=tenant, values=["а", "б"])
    before = (await pg_session.execute(select(repository.table))).mappings().all()
    mixed = await repository.resolve(equestrian_id=tenant, values=["б", "в", "а", "а"])
    assert mixed["а"] == first["а"] and mixed["б"] == first["б"]
    assert await repository.resolve(equestrian_id=tenant, values=list(mixed)) == mixed
    after = (await pg_session.execute(select(repository.table))).mappings().all()
    assert [row for row in after if row["id"] in first.values()] == before
    foreign = await repository.resolve(equestrian_id=other, values=list(mixed))
    assert set(foreign.values()).isdisjoint(mixed.values())
    values = [f"item-{i}" for i in range(999)] + ["я" * 100_000]
    batch = await repository.resolve(equestrian_id=tenant, values=values)
    assert len(batch) == 1000
    assert await repository.resolve(equestrian_id=tenant, values=values) == batch
    rows = (await pg_session.execute(select(repository.table))).mappings().all()
    assert all(row[repository.hash_column] == sha256(row[repository.value_column].encode()).digest() for row in rows)
    assert await repository.count_filtered(equestrian_id=tenant) == 1003
    assert await repository.count_filtered(equestrian_id=other) == 3
    assert (await repository.get_filtered(equestrian_id=tenant, search="я"))[0].model_dump()[
        repository.value_column
    ] == values[-1]


@pytest.mark.parametrize("repository_cls", REPOSITORIES)
async def test_overlapping_concurrent_batches_20_repeats(pg_engine: AsyncEngine, repository_cls):
    """IT-SEO-04: independent READ COMMITTED transactions, reversed lock order at input."""
    for _ in range(20):
        tenant = uuid4()
        barrier = asyncio.Barrier(2)

        async def resolve(values, tenant=tenant, barrier=barrier):
            async with AsyncSession(pg_engine) as session, session.begin():
                await session.execute(text("SET LOCAL lock_timeout = '5s'"))
                await session.execute(text("SET LOCAL statement_timeout = '10s'"))
                await barrier.wait()
                return await repository_cls(session).resolve(equestrian_id=tenant, values=values)

        left, right = await asyncio.wait_for(
            asyncio.gather(resolve(["а", "б", "в"]), resolve(["в", "б", "г"])), timeout=15
        )
        assert left["б"] == right["б"] and left["в"] == right["в"]
        async with AsyncSession(pg_engine) as session:
            assert await repository_cls(session).count_filtered(equestrian_id=tenant) == 4


@pytest.mark.parametrize("repository_cls", REPOSITORIES)
async def test_listing_search_sort_pagination(pg_session: AsyncSession, repository_cls):
    """IT-SEO-05: Cyrillic ILIKE, literal wildcards/backslash, stable ordering and total."""
    repository = repository_cls(pg_session)
    tenant, other = uuid4(), uuid4()
    values = ["Купить лошадь", "Аренда", "/a_b", "/axb", "/a%b", "/a\\b"]
    ids = await repository.resolve(equestrian_id=tenant, values=values)
    await repository.resolve(equestrian_id=other, values=["Купить лошадь", "foreign"])
    assert await repository.count_filtered(equestrian_id=uuid4()) == 0
    assert await repository.get_filtered(equestrian_id=uuid4()) == []
    for search, matched_value in [("  ЛОШАД  ", values[0]), ("a_b", "/a_b"), ("%", "/a%b"), ("\\", "/a\\b")]:
        rows = await repository.get_filtered(equestrian_id=tenant, search=search)
        assert [getattr(row, repository.value_column) for row in rows] == [matched_value]
        assert await repository.count_filtered(equestrian_id=tenant, search=search) == 1
    assert await repository.count_filtered(equestrian_id=tenant, search="   ") == len(values)
    for ordering in [repository.value_column, f"-{repository.value_column}"]:
        rows = await repository.get_filtered(equestrian_id=tenant, ordering=ordering)
        expected = (
            (
                await pg_session.execute(
                    select(repository.table.c.id)
                    .where(repository.table.c.equestrian_id == tenant)
                    .order_by(
                        repository.table.c[repository.value_column].desc()
                        if ordering.startswith("-")
                        else repository.table.c[repository.value_column].asc(),
                        repository.table.c.id.asc(),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert [row.id for row in rows] == expected
        page = await repository.get_filtered(equestrian_id=tenant, ordering=ordering, limit=2, offset=1)
        assert [row.id for row in page] == expected[1:3]
        assert await repository.count_filtered(equestrian_id=tenant) == len(values)
    # Explicit timestamp tie tests id DESC; then a newer timestamp takes precedence.
    now = datetime.now(UTC)
    await pg_session.execute(update(repository.table).values(created_at=now))
    rows = await repository.get_filtered(equestrian_id=tenant)
    assert [row.id for row in rows] == sorted(ids.values(), reverse=True)
    newest = min(ids.values())
    await pg_session.execute(
        update(repository.table).where(repository.table.c.id == newest).values(created_at=now + timedelta(seconds=1))
    )
    assert (await repository.get_filtered(equestrian_id=tenant))[0].id == newest
    with pytest.raises(ClientError):
        await repository.get_filtered(equestrian_id=tenant, ordering="created_at; DROP TABLE pages")


@pytest.mark.parametrize("repository_cls", REPOSITORIES)
async def test_missing_resolved_hash_maps_safe_error(pg_session: AsyncSession, repository_cls, monkeypatch):
    """A missing lookup after insert must fail closed, without returning partial UUIDs."""
    repository = repository_cls(pg_session)
    original = pg_session.execute
    calls = 0

    async def execute(statement, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            return await original(
                select(repository.table.c.id, repository.table.c[repository.hash_column]).where(False)
            )
        return await original(statement, *args, **kwargs)

    monkeypatch.setattr(pg_session, "execute", execute)
    with pytest.raises(RepositoryError, match="^Внутренняя ошибка сервиса$"):
        await repository.resolve(equestrian_id=UUID(int=1), values=["sensitive-value"])


@pytest.mark.parametrize("descending", [False, True])
async def test_pages_created_at_order(pg_session: AsyncSession, descending: bool):
    """IT-SP-01: full timestamps and same-direction UUID tiebreaker, pages only."""
    repository = PageRepository(pg_session)
    tenant = uuid4()
    ids = await repository.resolve(equestrian_id=tenant, values=["/a", "/b", "/c", "/d"])
    ordered_ids = sorted(ids.values())
    now = datetime(2026, 10, 20, 12, tzinfo=UTC)
    await pg_session.execute(update(repository.table).values(created_at=now))
    await pg_session.execute(
        update(repository.table)
        .where(repository.table.c.id == ordered_ids[0])
        .values(created_at=now + timedelta(microseconds=1))
    )
    expected = ordered_ids[1:] + ordered_ids[:1]
    if descending:
        expected.reverse()
    rows = await repository.get_filtered(equestrian_id=tenant, ordering="-created_at" if descending else "created_at")
    assert [row.id for row in rows] == expected
    for ordering in ["created_at", "-created_at"]:
        with pytest.raises(ClientError):
            await SearchQueryRepository(pg_session).get_filtered(equestrian_id=tenant, ordering=ordering)


@pytest.mark.parametrize("reverse_rank", [False, True])
async def test_pages_ranked_search_pagination(pg_session: AsyncSession, reverse_rank: bool):
    """IT-SP-02: either M4 direction; unmatched ranks never exclude unranked pages."""
    repository = PageRepository(pg_session)
    tenant, other = uuid4(), uuid4()
    ids = await repository.resolve(equestrian_id=tenant, values=["/a", "/ab", "/abc", "/abcd", "/x"])
    foreign = await repository.resolve(equestrian_id=other, values=["/a"])
    await pg_session.execute(update(repository.table).values(created_at=datetime.now(UTC)))
    known_ranks = [ids["/ab"], ids["/a"]]
    if reverse_rank:
        known_ranks.reverse()
    ranked = [foreign["/a"], uuid4(), ids["/x"], *known_ranks]
    expected = known_ranks + sorted([ids["/abc"], ids["/abcd"]], reverse=True)
    rows = await repository.get_filtered(equestrian_id=tenant, search="a", ranked_page_ids=ranked)
    assert [row.id for row in rows] == expected
    page = await repository.get_filtered(equestrian_id=tenant, search="a", ranked_page_ids=ranked, limit=2, offset=1)
    assert [row.id for row in page] == expected[1:3]
    assert await repository.count_filtered(equestrian_id=tenant, search="a") == 4
    empty_rank = await repository.get_filtered(equestrian_id=tenant, search="a", ranked_page_ids=[])
    assert [row.id for row in empty_rank] == sorted([ids[v] for v in ids if "a" in v], reverse=True)


@pytest.mark.parametrize("limit", [5, 50])
@pytest.mark.parametrize("with_rank", [False, True])
async def test_pages_two_sql_statements(pg_session: AsyncSession, sql_log: list[str], limit: int, with_rank: bool):
    """IT-SP-03: exactly one listing SELECT and one independent total SELECT."""
    repository = PageRepository(pg_session)
    tenant = uuid4()
    ids = await repository.resolve(equestrian_id=tenant, values=[f"/page-{i}" for i in range(60)])
    ranked = list(reversed(list(ids.values()))) if with_rank else None
    sql_log.clear()
    rows = await repository.get_filtered(equestrian_id=tenant, limit=limit, ranked_page_ids=ranked)
    total = await repository.count_filtered(equestrian_id=tenant)
    assert len(rows) == limit and total == 60
    assert len(sql_log) == 2
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in sql_log)
    assert "count(" in sql_log[1]
    if with_rank:
        assert "WITH ORDINALITY" in sql_log[0] and "NULLS LAST" in sql_log[0]
        assert "unnest(" in sql_log[0]


async def test_pages_rank_5000_uuids(pg_session: AsyncSession, sql_log: list[str]):
    """IT-SP-04: one SQL with one array parameter, even for a large ranking."""
    repository = PageRepository(pg_session)
    tenant = uuid4()
    ids = await repository.resolve(equestrian_id=tenant, values=[f"/page-{i}" for i in range(100)])
    expected = list(reversed(list(ids.values())))
    ranked = [uuid4() for _ in range(4900)] + expected
    sql_log.clear()
    rows = await repository.get_filtered(equestrian_id=tenant, ranked_page_ids=ranked, limit=50, offset=25)
    assert [row.id for row in rows] == expected[25:75]
    assert len(sql_log) == 1
    assert "WITH ORDINALITY" in sql_log[0]
    assert "UUID[]" in sql_log[0]
