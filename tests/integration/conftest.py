"""Caller-provided PostgreSQL only; never import runtime settings."""

import logging
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
async def empty_pg_engine() -> AsyncIterator[AsyncEngine]:
    raw_url = os.environ.get("TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("TEST_DATABASE_URL is required for dedicated PostgreSQL tests")
    url = make_url(raw_url)
    if url.drivername != "postgresql+asyncpg":
        pytest.fail("TEST_DATABASE_URL must use postgresql+asyncpg")
    schema = f"test_seo_{uuid4().hex}"
    admin = create_async_engine(url, poolclass=NullPool, hide_parameters=True)
    engine = create_async_engine(
        url,
        poolclass=NullPool,
        hide_parameters=True,
        connect_args={"server_settings": {"search_path": f"{schema},public"}},
    )
    created = False
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        created = True
        yield engine
    finally:
        await engine.dispose()
        if created and os.environ.get("TEST_KEEP_SCHEMA") != "1":
            # schema is generated locally, never derived from caller input.
            assert schema.startswith("test_seo_") and len(schema) == 41
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
def migration_config(tmp_path: Path) -> Config:
    """Use real revision files with a test-only env and explicit schema/version table."""
    (tmp_path / "env.py").write_text(
        "from alembic import context\n"
        "connection = context.config.attributes['connection']\n"
        "context.configure(connection=connection, "
        "version_table_schema=context.config.attributes['schema'])\n"
        "with context.begin_transaction():\n"
        "    context.run_migrations()\n"
    )
    config = Config()
    config.set_main_option("path_separator", "os")
    config.set_main_option("script_location", str(tmp_path))
    config.set_main_option("version_locations", str(Path(__file__).resolve().parents[2] / "src/migration/versions"))
    return config


@pytest.fixture
async def pg_engine(empty_pg_engine: AsyncEngine, migration_config: Config) -> AsyncEngine:
    async with empty_pg_engine.begin() as connection:
        migration_config.attributes["schema"] = await connection.scalar(text("SELECT current_schema()"))

        def upgrade(sync_connection) -> None:
            migration_config.attributes["connection"] = sync_connection
            command.upgrade(migration_config, "head")

        await connection.run_sync(upgrade)
    return empty_pg_engine


@pytest.fixture
async def pg_session(pg_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    async with AsyncSession(pg_engine) as session:
        yield session
        await session.rollback()


@pytest.fixture
def sql_log(pg_engine: AsyncEngine, caplog: pytest.LogCaptureFixture) -> Iterator[list[str]]:
    """Record statements only, never bound values or connection configuration."""
    statements: list[str] = []
    logger = logging.getLogger("tests.seo.sql")
    caplog.set_level(logging.INFO, logger=logger.name)

    def record(_conn, _cursor, statement, _parameters, _context, _executemany) -> None:
        statements.append(statement)
        logger.info("SQL: %s", statement)

    event.listen(pg_engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(pg_engine.sync_engine, "before_cursor_execute", record)
