"""Concrete batch composition shares one session, never the request resource."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.services.normalization_finalization_service import NormalizationFinalizationService
from core.services.normalization_scan_service import NormalizationScanService
from core.services.page_normalizer import PageNormalizer
from depends.normalization_runtime import NormalizationBatchTransactions
from repositories.normalization_repository import NormalizationRepository
from repositories.page_repository import PageRepository


def build(**kwargs):
    sessions = [AsyncMock(), AsyncMock()]
    for session in sessions:
        session.__aenter__.return_value = session
        session.begin = MagicMock(return_value=AsyncMock())
    factory = MagicMock(side_effect=sessions)
    return NormalizationBatchTransactions(factory, normalizer=PageNormalizer(), **kwargs), sessions, factory


async def test_scan_and_finalization_have_distinct_fresh_sessions_and_commit_boundaries():
    batches, sessions, factory = build(scan_batch_size=2, delete_batch_size=3)
    scan_tx, delete_tx = [session.begin.return_value for session in sessions]
    async with batches.scan() as scan:
        assert isinstance(scan, NormalizationScanService)
        assert isinstance(scan._repository, NormalizationRepository)
        assert isinstance(scan._pages, PageRepository)
        assert scan._repository.session is sessions[0]
        assert scan._pages.session is sessions[0]
        assert scan._batch_size == 2
        scan_tx.__aexit__.assert_not_awaited()
    scan_tx.__aexit__.assert_awaited_once_with(None, None, None)
    async with batches.finalization() as finalization:
        assert isinstance(finalization, NormalizationFinalizationService)
        assert isinstance(finalization._repository, NormalizationRepository)
        assert finalization._repository.session is sessions[1]
        assert finalization._batch_size == 3
    delete_tx.__aexit__.assert_awaited_once_with(None, None, None)
    assert factory.call_count == 2
    for session in sessions:
        session.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("stage", ["scan", "finalization"])
@pytest.mark.parametrize("error", [ValueError("invalid"), RuntimeError("storage"), asyncio.CancelledError()])
async def test_batch_errors_and_cancellation_rollback(stage, error):
    batches, sessions, _ = build()
    session = sessions[0]
    session.begin.return_value.__aexit__.return_value = False
    with pytest.raises(type(error)):
        async with getattr(batches, stage)():
            raise error
    assert session.begin.return_value.__aexit__.call_args.args[0] is type(error)
    session.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("stage", ["scan", "finalization"])
async def test_commit_failures_propagate_and_close_sessions(stage):
    batches, sessions, _ = build()
    sessions[0].begin.return_value.__aexit__.side_effect = RuntimeError("commit")
    with pytest.raises(RuntimeError, match="commit"):
        async with getattr(batches, stage)():
            pass
    sessions[0].__aexit__.assert_awaited_once()


@pytest.mark.parametrize("kwargs", [{"scan_batch_size": 0}, {"delete_batch_size": 501}])
def test_bounds_rejected_before_opening_session(kwargs):
    with pytest.raises(ValueError):
        build(**kwargs)
