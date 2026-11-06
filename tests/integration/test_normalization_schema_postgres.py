"""Real PostgreSQL schema slice of IT-N-01 (repository/race acceptance follows)."""

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from models.normalization import (
    normalization_mappings as mappings,
)
from models.normalization import (
    normalization_revisions as revisions,
)
from models.normalization import (
    normalization_scans as scans,
)
from models.normalization import (
    normalization_snapshots as snapshots,
)
from models.normalization import (
    normalization_tenants as tenants,
)
from models.pages import pages

pytestmark = pytest.mark.infrastructure


async def test_normalization_upgrade_backfills_pages_and_preserves_existing_data(
    empty_pg_engine: AsyncEngine,
    migration_config: Config,
) -> None:
    async with empty_pg_engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))

        def upgrade_to(conn: Connection, target: str) -> None:
            migration_config.attributes.update(connection=conn, schema=schema)
            command.upgrade(migration_config, target)

        await connection.run_sync(upgrade_to, "20261028_0003")
        tenant = uuid4()
        # Use old SQL, deliberately no scan_seq column before upgrade.
        for index in range(5):
            await connection.execute(
                text("INSERT INTO pages (id, equestrian_id, page, page_hash) VALUES (:id, :tenant, :page, :hash)"),
                {"id": uuid4(), "tenant": tenant, "page": f"/{index}", "hash": bytes([index])},
            )
        await connection.run_sync(upgrade_to, "head")
        old = (await connection.execute(select(pages).order_by(pages.c.scan_seq))).mappings().all()
        assert len(old) == 5 and len({r["scan_seq"] for r in old}) == 5
        new_seq = await connection.scalar(
            insert(pages)
            .values(
                id=uuid4(),
                equestrian_id=tenant,
                page="/new",
                page_hash=b"new",
            )
            .returning(pages.c.scan_seq)
        )
        assert new_seq > max(r["scan_seq"] for r in old)


async def test_normalization_tenant_uniqueness_checkpoint_and_rollback_guard(
    pg_engine: AsyncEngine,
    migration_config: Config,
) -> None:
    async with pg_engine.begin() as connection:
        tenant, other, first, second = (uuid4() for _ in range(4))
        await connection.execute(insert(tenants), [{"equestrian_id": tenant}, {"equestrian_id": other}])
        await connection.execute(
            insert(revisions),
            [
                {"equestrian_id": tenant, "operation_id": first, "revision": 1, "status": "scanning"},
                {"equestrian_id": tenant, "operation_id": second, "revision": 2, "status": "pending"},
            ],
        )

        async def rejected(statement) -> None:
            with pytest.raises(IntegrityError):
                async with connection.begin_nested():
                    await connection.execute(statement)

        await rejected(update(revisions).where(revisions.c.operation_id == second).values(status="scanning"))
        await rejected(
            insert(snapshots).values(
                equestrian_id=other,
                operation_id=first,
                query_params=[],
                hashes=[],
            )
        )
        await rejected(insert(scans).values(equestrian_id=tenant, operation_id=first, sealed=1))
        await connection.execute(insert(scans).values(equestrian_id=tenant, operation_id=first, high_water=10))
        await rejected(update(scans).values(cursor=11))
        source, target = uuid4(), uuid4()
        values = dict(
            equestrian_id=tenant,
            operation_id=first,
            source_id=source,
            target_id=target,
            source_scan_seq=1,
            part_index=0,
        )
        await connection.execute(insert(mappings).values(**values))
        await rejected(insert(mappings).values(**(values | {"target_id": uuid4()})))
        await rejected(
            insert(mappings).values(
                **(values | {"source_id": uuid4(), "target_id": source, "source_scan_seq": 2, "part_index": -1})
            )
        )
        schema = await connection.scalar(text("SELECT current_schema()"))

        def guarded_downgrade(conn: Connection) -> None:
            migration_config.attributes.update(connection=conn, schema=schema)
            command.downgrade(migration_config, "20261028_0003")

        with pytest.raises(DBAPIError, match="normalization ledger is nonempty"):
            async with connection.begin_nested():
                await connection.run_sync(guarded_downgrade)
        assert await connection.scalar(select(mappings.c.source_id)) == source
        assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "20261102_0004"
