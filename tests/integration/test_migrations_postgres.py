"""IT-SEO-01: real Alembic upgrade/downgrade confined to a unique schema."""

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

import models  # noqa: F401
from utils.basemodel import metadata

pytestmark = pytest.mark.infrastructure

TABLES = {"search_queries", "pages", "blacklisted_query_params", "blacklisted_hashes", "parse_dispatches"}


async def test_migration_round_trip(empty_pg_engine: AsyncEngine, migration_config: Config) -> None:
    async with empty_pg_engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
        assert schema.startswith("test_seo_")
        migration_config.attributes["schema"] = schema

        def round_trip(conn: Connection) -> None:
            migration_config.attributes["connection"] = conn
            for target in ("head", "-1", "head"):
                if target == "-1":
                    command.downgrade(migration_config, "20260710_0001")
                    assert set(inspect(conn).get_table_names(schema=schema)) == {"alembic_version"}
                    assert conn.scalar(text("SELECT extname FROM pg_extension WHERE extname='pg_trgm'")) == "pg_trgm"
                    continue
                command.upgrade(migration_config, target)
                inspector = inspect(conn)
                assert set(inspector.get_table_names(schema=schema)) == TABLES | {"alembic_version"}
                assert conn.scalar(text(f'SELECT version_num FROM "{schema}".alembic_version')) == "20261028_0003"
                for table in TABLES:
                    indexes = {index["name"]: index for index in inspector.get_indexes(table, schema=schema)}
                    if table in ("pages", "search_queries"):
                        field = "page" if table == "pages" else "query"
                        unique = f"ux_{table}_equestrian_id_{field}_hash"
                        assert indexes[unique]["unique"]
                        assert indexes[unique]["column_names"] == ["equestrian_id", f"{field}_hash"]
                        trgm = indexes[f"ix_{table}_{field}_trgm"]["dialect_options"]
                        assert trgm["postgresql_using"] == "gin"
                        assert trgm["postgresql_ops"] == {field: "gin_trgm_ops"}
                        ordering = indexes[f"ix_{table}_equestrian_id_created_at"]
                        assert ordering["column_names"] == ["equestrian_id", "created_at", "id"]
                        assert ordering["column_sorting"] == {"created_at": ("desc",), "id": ("desc",)}
                    elif table != "parse_dispatches":
                        index = indexes[f"ux_{table}_equestrian_id_value"]
                        assert index["unique"] and index["column_names"] == ["equestrian_id", "value"]
                # Compare ORM with the migrated schema, excluding public/runtime tables.
                context = MigrationContext.configure(
                    conn,
                    opts={
                        "include_object": lambda obj, name, kind, reflected, compare_to: (
                            kind != "table" or name in TABLES
                        ),
                    },
                )
                assert compare_metadata(context, metadata) == []

        await connection.run_sync(round_trip)


async def test_it_ps_01_ledger_constraints_and_reversible_migration(
    pg_engine: AsyncEngine,
    migration_config: Config,
) -> None:
    """IT-PS-01: real partial index, CHECKs and downgrade preserving old tables."""
    from datetime import date
    from uuid import uuid4

    from sqlalchemy import insert
    from sqlalchemy.exc import IntegrityError

    from models.parse_dispatches import parse_dispatches

    async with pg_engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
        assert schema.startswith("test_seo_")

        def inspect_ledger(conn: Connection) -> None:
            inspector = inspect(conn)
            indexes = {i["name"]: i for i in inspector.get_indexes("parse_dispatches", schema=schema)}
            index = indexes["ux_parse_dispatches_schedule"]
            assert index["unique"]
            assert index["column_names"] == ["provider", "equestrian_id", "schedule_date"]
            assert "schedule" in index["dialect_options"]["postgresql_where"]
            checks = {c["name"] for c in inspector.get_check_constraints("parse_dispatches", schema=schema)}
            assert checks == {
                f"ck_parse_dispatches_{name}" for name in ("trigger", "status", "attempts", "schedule_date")
            }

        await connection.run_sync(inspect_ledger)
        values = dict(
            provider="demo",
            equestrian_id=uuid4(),
            trigger="schedule",
            schedule_date=date(2026, 10, 28),
            status="pending",
            attempts=1,
        )
        await connection.execute(insert(parse_dispatches).values(id=uuid4(), **values))
        invalid: list[dict[str, object]] = [
            {},
            {"schedule_date": None},
            {"trigger": "manual"},
            {"trigger": "invalid"},
            {"status": "invalid"},
            {"attempts": 0},
        ]
        for overrides in invalid:
            with pytest.raises(IntegrityError):
                async with connection.begin_nested():
                    await connection.execute(insert(parse_dispatches).values(id=uuid4(), **(values | overrides)))
        for _ in range(2):
            await connection.execute(
                insert(parse_dispatches).values(
                    id=uuid4(), **(values | {"trigger": "manual", "schedule_date": None, "status": "published"})
                )
            )
        # An existing SEO row must survive removing only the ledger revision.
        old_id = uuid4()
        await connection.execute(
            text("INSERT INTO blacklisted_hashes (id, equestrian_id, value) VALUES (:id, :tenant, 'kept')"),
            {"id": old_id, "tenant": values["equestrian_id"]},
        )

        def downgrade_upgrade(conn: Connection) -> None:
            migration_config.attributes.update(connection=conn, schema=schema)
            command.downgrade(migration_config, "20261023_0002")
            assert "parse_dispatches" not in inspect(conn).get_table_names(schema=schema)
            assert conn.scalar(text("SELECT value FROM blacklisted_hashes WHERE id=:id"), {"id": old_id}) == "kept"
            command.upgrade(migration_config, "head")
            assert "parse_dispatches" in inspect(conn).get_table_names(schema=schema)

        await connection.run_sync(downgrade_upgrade)
