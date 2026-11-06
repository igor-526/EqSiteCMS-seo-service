"""Short caller-owned transaction adapter; never span a network operation."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol
from repositories.normalization_repository import NormalizationRepository


class NormalizationTransaction:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[NormalizationRepositoryProtocol]:
        # begin() rolls back on *all* exceptions, including cancellation. Commit
        # failures propagate; no inbox acknowledgement can precede this exit.
        async with self._session_factory() as session, session.begin():
            yield NormalizationRepository(session)
