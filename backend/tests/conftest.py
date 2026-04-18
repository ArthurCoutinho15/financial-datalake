import sys
import os

sys.path.append(os.getcwd())
sys.path.append(os.path.abspath("."))

import pytest_asyncio

from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.main import app
from src.core.deps import get_session
from src.core.configs import settings

DATABASE_URL = "sqlite+aiosqlite:///./test.db"

engine_test = create_async_engine(DATABASE_URL, echo=False)

TestingSessionlocal = sessionmaker(
    engine_test, class_=AsyncSession, expire_on_commit=False
)


@pytest_asyncio.fixture(scope="function")
async def create_db():
    async with engine_test.begin() as conn:
        await conn.run_sync(settings.DBBaseModel.metadata.create_all)
    yield
    async with engine_test.begin() as conn:
        await conn.run_sync(settings.DBBaseModel.metadata.drop_all)
    await engine_test.dispose()


async def override_get_session():
    async with TestingSessionlocal() as session:
        yield session


@pytest_asyncio.fixture
async def client(create_db):
    app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
