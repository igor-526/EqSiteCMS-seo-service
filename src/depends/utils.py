from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from utils.database import SessionFactory


async def get_session() -> AsyncGenerator[AsyncSession]:
    """Own session lifetime only; the request route owns the transaction."""
    async with SessionFactory() as session:
        yield session
