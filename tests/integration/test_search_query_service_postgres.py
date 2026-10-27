"""UT-SQ-05: actual service/repository SQL count, tenant and bounded peer calls."""

from datetime import date, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.schemas.clients.ya_metrica import SearchQueryStatsRanking
from repositories import SearchQueryRepository
from tests.unit.test_search_query_service import make_listing_service

pytestmark = pytest.mark.infrastructure


@pytest.mark.parametrize("size", [0, 5, 50, 100])
@pytest.mark.parametrize("buckets", [1, 31])
@pytest.mark.parametrize("ranked", [False, True])
async def test_ut_sq_05_composed_two_sql_bounded_peers(pg_session, sql_log, size, buckets, ranked):
    tenant, foreign_tenant = uuid4(), uuid4()
    repository = SearchQueryRepository(pg_session)
    ids = await repository.resolve(equestrian_id=tenant, values=[f"query-{i:03}" for i in range(size)])
    foreign = await repository.resolve(equestrian_id=foreign_tenant, values=["query-foreign"])
    peer = AsyncMock()
    service, _, peer = make_listing_service(repository, peer)
    # A foreign/unknown peer UUID must not become a SEO row.
    ranked_ids = [*foreign.values(), uuid4(), *reversed(list(ids.values()))]
    peer.query_ranking.return_value = SearchQueryStatsRanking(
        metric="visits", direction="desc", search_query_ids=ranked_ids
    )
    sql_log.clear()
    result = await service.list_filtered(
        equestrian_id=tenant,
        search=" query- ",
        ordering="-metric:2026-10-26" if ranked else None,
        date_from=date(2026, 10, 26) - timedelta(days=buckets - 1),
        date_to=date(2026, 10, 26),
        limit=max(5, size),
    )
    assert len(sql_log) == 2
    assert result.total == size and len(result.items) == size
    assert {item.id for item in result.items} == set(ids.values())
    if ranked:
        assert [item.id for item in result.items] == list(reversed(list(ids.values())))
    assert len(result.metrics_table.columns) == buckets
    assert peer.query_ranking.await_count == int(ranked)
    assert peer.query_table.await_count == int(size > 0)
    assert len(peer.mock_calls) <= 2
    if size:
        args = peer.query_table.call_args.kwargs
        assert args["search_query_ids"] == [item.id for item in result.items]
        assert args["equestrian_id"] == tenant
        assert len(args["columns"]) == buckets
    if ranked:
        assert peer.query_ranking.call_args.kwargs["equestrian_id"] == tenant
