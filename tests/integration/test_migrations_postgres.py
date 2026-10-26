"""IT-SEO-01: real Alembic upgrade/downgrade confined to a unique schema."""

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

import models  # noqa: F401
from utils.basemodel import metadata

TABLES = {"search_queries", "pages", "blacklisted_query_params", "blacklisted_hashes"}


async def test_migration_round_trip(empty_pg_engine: AsyncEngine, migration_config: Config) -> None:
    async with empty_pg_engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
        assert schema.startswith("test_seo_")
        migration_config.attributes["schema"] = schema

        def round_trip(conn: Connection) -> None:
            migration_config.attributes["connection"] = conn
            for target in ("head", "-1", "head"):
                if target == "-1":
                    command.downgrade(migration_config, target)
                    assert set(inspect(conn).get_table_names(schema=schema)) == {"alembic_version"}
                    assert conn.scalar(text("SELECT extname FROM pg_extension WHERE extname='pg_trgm'")) == "pg_trgm"
                    continue
                command.upgrade(migration_config, target)
                inspector = inspect(conn)
                assert set(inspector.get_table_names(schema=schema)) == TABLES | {"alembic_version"}
                assert conn.scalar(text(f'SELECT version_num FROM "{schema}".alembic_version')) == "20261023_0002"
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
                    else:
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
