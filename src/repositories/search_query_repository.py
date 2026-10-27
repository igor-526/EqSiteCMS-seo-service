from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import ColumnElement, bindparam, column, func, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from core.entities import SearchQuery
from models import search_queries

from .abstract_repository import ResolvableRepository, repository_errors


class SearchQueryRepository(ResolvableRepository[SearchQuery]):
    table = search_queries
    entity = SearchQuery
    value_column = "query"
    hash_column = "query_hash"

    def _get_order(self, ordering: str | None) -> list[ColumnElement]:
        if ordering == "created_at":
            return [self.table.c.created_at.asc(), self.table.c.id.asc()]
        if ordering == "-created_at":
            return [self.table.c.created_at.desc(), self.table.c.id.desc()]
        return super()._get_order(ordering)

    async def get_filtered(
        self,
        *,
        equestrian_id: UUID,
        search: str | None = None,
        ordering: str | None = None,
        limit: int = 20,
        offset: int = 0,
        ranked_search_query_ids: Sequence[UUID] | None = None,
    ) -> list[SearchQuery]:
        if ranked_search_query_ids is None:
            return await super().get_filtered(
                equestrian_id=equestrian_id, search=search, ordering=ordering, limit=limit, offset=offset
            )
        # One typed array parameter, independent of ranking size (no IN/CASE expansion).
        ranked = (
            func.unnest(bindparam("ranked", type_=ARRAY(PG_UUID(as_uuid=True))))
            .table_valued(column("search_query_id", PG_UUID(as_uuid=True)), with_ordinality="ordinal")
            .render_derived(name="ranked_queries")
        )
        statement = (
            select(self.table)
            .select_from(self.table.outerjoin(ranked, self.table.c.id == ranked.c.search_query_id))
            .where(*self._filter_conditions(equestrian_id=equestrian_id, search=search))
            .order_by(ranked.c.ordinal.asc().nulls_last(), *self._get_order(None))
            .limit(limit)
            .offset(offset)
        )
        with repository_errors():
            rows = await self.session.execute(statement, {"ranked": list(ranked_search_query_ids)})
            return [self.entity.model_validate(dict(row)) for row in rows.mappings()]
