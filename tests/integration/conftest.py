"""Caller-provided PostgreSQL only; never import runtime settings."""

import logging
import os
import sys
from collections.abc import AsyncIterator, Iterator
from importlib import import_module
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def qa_infra_context(request: pytest.FixtureRequest) -> dict:
    """Infrastructure requires the explicit shared helper AND a runner-owned permit.

    Run exact node IDs through scripts/qa/infra/run.py; direct TEST_DATABASE_URL /
    TEST_NATS_URL invocation is deliberately unsupported. No runtime fallback.
    """
    root = os.environ.get("QA_TEST_SUPPORT_ROOT")
    if not root or not (Path(root) / "infra/__init__.py").is_file():
        pytest.fail("Explicit QA_TEST_SUPPORT_ROOT with infra support is required")
    sys.path.insert(0, str(Path(root).resolve()))
    load_context = import_module("infra").load_context

    kind = "nats" if request.node.path.name == "test_nats_broker.py" else "pg"
    return load_context(kind)


@pytest.fixture
async def empty_pg_engine(qa_infra_context: dict) -> AsyncIterator[AsyncEngine]:
    """Use the already-created exact schema; root cleanup belongs only to runner."""
    url = make_url(qa_infra_context["url"]).set(drivername="postgresql+asyncpg")
    engine = create_async_engine(
        url,
        poolclass=NullPool,
        hide_parameters=True,
        connect_args=qa_infra_context["connect_args"],
    )
    try:
        yield engine
    finally:
        await engine.dispose()


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
