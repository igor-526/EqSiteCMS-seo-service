"""Caller transaction ownership; mocks do not replace real PostgreSQL acceptance."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from repositories.normalization_transaction import NormalizationTransaction


def adapter():
    session = AsyncMock()
    transaction = AsyncMock()
    # AsyncSession.begin() is synchronous, returning an async context manager.
    session.begin = MagicMock(return_value=transaction)
    session.__aenter__.return_value = session
    factory = MagicMock(return_value=session)
    return NormalizationTransaction(factory), session, transaction


async def test_new_session_and_transaction_per_call():
    transaction, session, tx = adapter()
    async with transaction() as first:
        assert first is not None
        tx.__aexit__.assert_not_awaited()
    tx.__aexit__.assert_awaited_once_with(None, None, None)
    async with transaction() as second:
        assert second is not first
    assert session.begin.call_count == 2
    assert session.__aexit__.await_count == 2


@pytest.mark.parametrize("error", [ValueError("invalid"), RuntimeError("database"), asyncio.CancelledError()])
async def test_errors_and_cancellation_reach_transaction_exit(error):
    transaction, session, tx = adapter()
    tx.__aexit__.return_value = False
    with pytest.raises(type(error)):
        async with transaction():
            raise error
    assert tx.__aexit__.call_args.args[0] is type(error)
    assert tx.__aexit__.call_args.args[1] is error
    session.__aexit__.assert_awaited_once()


async def test_commit_failure_propagates_and_closes_session():
    transaction, session, tx = adapter()
    tx.__aexit__.side_effect = RuntimeError("commit")
    with pytest.raises(RuntimeError, match="commit"):
        async with transaction():
            pass
    session.__aexit__.assert_awaited_once()
