from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from src.core.configs import settings

engine: AsyncEngine | None = None
SessionLocal = None


async def get_engine():
    global engine
    if engine is None:
        engine = create_async_engine(settings.DB_URL)
    return engine


async def get_session():
    global SessionLocal

    engine = await get_engine()

    if SessionLocal is None:
        SessionLocal = sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )

    async with SessionLocal() as session:  
        yield session