from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_session as db_get_session


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async for session in db_get_session():
        try:
            yield session
        finally:
            await session.close()
