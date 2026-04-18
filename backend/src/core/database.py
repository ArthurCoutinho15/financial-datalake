from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from src.core.configs import settings

engine: AsyncEngine = None


async def get_engine():
    global engine
    if engine is None:
        engine = create_async_engine(settings.DB_URL)
    return engine


async def get_session():
    engine = await get_engine()
    async with sessionmaker(
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
        class_=AsyncSession,
        bind=engine,
    ) as session:
        yield session
