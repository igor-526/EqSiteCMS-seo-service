"""IT-SEO-02: tenant uniqueness, unbounded text and digest storage."""

import hashlib
import random
import string
from uuid import uuid4

import pytest
from pydantic import BaseModel
from sqlalchemy import Table, func, select
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import BlacklistedHash, BlacklistedQueryParam, Page, SearchQuery
from models import blacklisted_hashes, blacklisted_query_params, pages, search_queries


@pytest.mark.parametrize(
    ("table", "field", "entity"),
    [
        (search_queries, "query", SearchQuery),
        (pages, "page", Page),
    ],
)
async def test_text_hash_and_tenant_uniqueness(
    pg_session: AsyncSession, table: Table, field: str, entity: type[BaseModel]
) -> None:
    tenant = uuid4()
    # Low-compressibility text also exceeds PostgreSQL's B-tree entry size.
    rng = random.Random(42)
    value = "Лошадь/" + "".join(rng.choices(string.ascii_letters + string.digits, k=99993))
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    values = {"equestrian_id": tenant, field: value, f"{field}_hash": digest}
    row = (await pg_session.execute(table.insert().values(**values).returning(table))).mappings().one()
    assert row[field] == value and len(value) == 100_000
    assert row[f"{field}_hash"] == digest and len(digest) == 32
    assert row["created_at"].tzinfo is not None and row["updated_at"] is None
    assert entity.model_validate(row).model_dump()["equestrian_id"] == tenant
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(table.insert().values(**values))
    other = (
        await pg_session.execute(table.insert().values(**(values | {"equestrian_id": uuid4()})).returning(table.c.id))
    ).scalar_one()
    assert other != row["id"]
    assert await pg_session.scalar(select(func.count()).select_from(table)) == 2


async def test_independent_blacklists(pg_session: AsyncSession) -> None:
    tenant = uuid4()
    for table, entity in ((blacklisted_query_params, BlacklistedQueryParam), (blacklisted_hashes, BlacklistedHash)):
        values = {"equestrian_id": tenant, "value": "top"}
        row = (await pg_session.execute(table.insert().values(**values).returning(table))).mappings().one()
        assert entity.model_validate(row).value == "top"
        with pytest.raises(IntegrityError):
            async with pg_session.begin_nested():
                await pg_session.execute(table.insert().values(**values))
        await pg_session.execute(table.insert().values(equestrian_id=uuid4(), value="top"))
        await pg_session.execute(table.insert().values(equestrian_id=tenant, value="x" * 255))
        with pytest.raises(DBAPIError) as error:
            async with pg_session.begin_nested():
                await pg_session.execute(table.insert().values(equestrian_id=tenant, value="x" * 256))
        assert "value too long" in str(error.value)
