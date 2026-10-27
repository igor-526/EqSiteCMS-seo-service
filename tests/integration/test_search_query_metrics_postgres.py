"""IT-SQ-04 / repository part of UT-SQ-05; caller-provided PostgreSQL only."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Numeric, column, select, update, values
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession

from repositories import SearchQueryRepository

pytestmark = pytest.mark.infrastructure


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("field", ["query", "created_at"])
async def test_global_field_order(pg_session: AsyncSession, descending: bool, field: str) -> None:
    repository = SearchQueryRepository(pg_session)
    tenant = uuid4()
    ids = await repository.resolve(equestrian_id=tenant, values=[f"query-{i:02}" for i in range(19)])
    await repository.resolve(equestrian_id=uuid4(), values=list(ids))
    now = datetime(2026, 10, 20, 12, tzinfo=UTC)
    for i, row_id in enumerate(ids.values()):
        # Same-day microseconds and ties; never sort by the displayed date.
        await pg_session.execute(
            update(repository.table)
            .where(repository.table.c.id == row_id)
            .values(created_at=now + timedelta(microseconds=i // 2))
        )
    table = repository.table
    direction = table.c[field].desc() if descending else table.c[field].asc()
    tie = table.c.id.desc() if descending and field == "created_at" else table.c.id.asc()
    expected = list(
        (
            await pg_session.scalars(
                select(table.c.id).where(table.c.equestrian_id == tenant).order_by(direction, tie)
            )
        ).all()
    )
    result: list[UUID] = []
    for offset in range(0, 20, 5):
        page = await repository.get_filtered(
            equestrian_id=tenant, ordering=("-" if descending else "") + field, limit=5, offset=offset
        )
        assert [row.id for row in page] == expected[offset : offset + 5]
        result.extend(row.id for row in page)
    assert result == expected and len(set(result)) == 19
    assert await repository.count_filtered(equestrian_id=tenant) == 19


@pytest.mark.parametrize("descending", [False, True])
async def test_numeric_rank_global_search_nulls(
    pg_session: AsyncSession, sql_log: list[str], descending: bool
) -> None:
    repository = SearchQueryRepository(pg_session)
    tenant, foreign_tenant = uuid4(), uuid4()
    ids = await repository.resolve(equestrian_id=tenant, values=[f"query-{i:02}" for i in range(19)] + ["excluded"])
    foreign = await repository.resolve(equestrian_id=foreign_tenant, values=["query-foreign"])
    now = datetime(2026, 10, 20, 12, tzinfo=UTC)
    await pg_session.execute(update(repository.table).values(created_at=now))
    unranked = [ids[f"query-{i:02}"] for i in range(15, 19)]
    await pg_session.execute(
        update(repository.table)
        .where(repository.table.c.id == unranked[0])
        .values(created_at=now + timedelta(seconds=1))
    )
    numbers = [
        Decimal(v) for v in ["2", "10", "100", "0", "1", "2", "10.5", "0.1", "11", "9", "99", "2.01", "1", "1000", "5"]
    ]
    # Emulate the peer's numeric SQL ranking; SEO only consumes ordered UUIDs.
    stats = values(column("id", PG_UUID(as_uuid=True)), column("value", Numeric()), name="stats").data(
        [(ids[f"query-{i:02}"], value) for i, value in enumerate(numbers)]
    )
    ranked = list(
        (
            await pg_session.scalars(
                select(stats.c.id).order_by(
                    stats.c.value.desc() if descending else stats.c.value.asc(), stats.c.id.asc()
                )
            )
        ).all()
    )
    expected = ranked + [unranked[0]] + sorted(unranked[1:], reverse=True)
    peer_ids = [foreign["query-foreign"], uuid4(), ids["excluded"], *ranked]
    result: list[UUID] = []
    for offset in range(0, 20, 5):
        sql_log.clear()
        page = await repository.get_filtered(
            equestrian_id=tenant, search="query", ranked_search_query_ids=peer_ids, limit=5, offset=offset
        )
        assert await repository.count_filtered(equestrian_id=tenant, search="query") == 19
        assert len(sql_log) == 2
        assert "WITH ORDINALITY" in sql_log[0] and "NULLS LAST" in sql_log[0] and "UUID[]" in sql_log[0]
        assert "JOIN" not in sql_log[1]
        assert [row.id for row in page] == expected[offset : offset + 5]
        result.extend(row.id for row in page)
    assert result == expected and len(set(result)) == 19
    empty_rank = await repository.get_filtered(equestrian_id=tenant, search="query", ranked_search_query_ids=[])
    assert [row.id for row in empty_rank] == [unranked[0]] + sorted(
        set(ids.values()) - {ids["excluded"], unranked[0]}, reverse=True
    )


@pytest.mark.parametrize("limit", [5, 50, 100])
@pytest.mark.parametrize("ranked", [False, True])
async def test_two_sql_and_literal_search(
    pg_session: AsyncSession, sql_log: list[str], limit: int, ranked: bool
) -> None:
    repository = SearchQueryRepository(pg_session)
    tenant = uuid4()
    ids = await repository.resolve(
        equestrian_id=tenant, values=[f"query-{i:03}" for i in range(115)] + ["а%_\\б", "аXXб"]
    )
    ranks = list(ids.values()) if ranked else None
    for search, count in [(None, 117), ("%", 1), ("_", 1), ("\\", 1), ("А%_\\Б", 1), ("not-found", 0)]:
        sql_log.clear()
        page = await repository.get_filtered(
            equestrian_id=tenant, search=search, limit=limit, ranked_search_query_ids=ranks
        )
        assert await repository.count_filtered(equestrian_id=tenant, search=search) == count
        assert len(page) == min(limit, count)
        assert len(sql_log) == 2
        assert "count(" in sql_log[1] and "JOIN" not in sql_log[1]
