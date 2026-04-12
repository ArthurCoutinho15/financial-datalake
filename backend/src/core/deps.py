from typing import AsyncGenerator, Optional
from pydantic import BaseModel

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import Session


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    session: AsyncSession = Session()

    try:
        yield session
    finally:
        await session.close()
