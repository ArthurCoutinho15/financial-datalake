import sys
import os
from pathlib import Path
from datetime import datetime

sys.path.append(os.getcwd())
sys.path.append(os.path.abspath("."))

import pytest_asyncio
import pandas as pd

from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.main import app
from src.core.deps import get_session
from src.core.configs import settings
from src.models.clients_model import ClientsModel
from src.models.portfolios_model import PortfoliosModel
from src.models.positions_model import PositionsModel
from src.models.transactions_model import TransactionsModel

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


async def load_seed_data(session: AsyncSession):
    """Load data from CSV files into the database"""
    seeds_dir = Path(__file__).parent / "seeds"
    
    # Load clients
    clients_df = pd.read_csv(seeds_dir / "fake_data_clients.csv")
    for _, row in clients_df.iterrows():
        client = ClientsModel(
            id=row["id"],
            name=row["name"],
            email=row["email"],
            cpf=row["cpf"],
            phone=row["phone"],
            city=row["city"],
            state=row["state"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"])
        )
        session.add(client)
    
    await session.flush()
    
    # Load portfolios
    portfolios_df = pd.read_csv(seeds_dir / "fake_data_portfolios.csv")
    for _, row in portfolios_df.iterrows():
        portfolio = PortfoliosModel(
            id=row["id"],
            client_id=row["client_id"],
            name=row["name"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"])
        )
        session.add(portfolio)
    
    await session.flush()
    
    # Load positions
    positions_df = pd.read_csv(seeds_dir / "fake_data_positions.csv")
    for _, row in positions_df.iterrows():
        position = PositionsModel(
            id=row["id"],
            portfolio_id=row["portfolio_id"],
            ticker=row["ticker"],
            asset_type=row["asset_type"],
            quantity=row["quantity"],
            avg_price_brl=row["avg_price_brl"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"])
        )
        session.add(position)
    
    await session.flush()
    
    # Load transactions
    transactions_df = pd.read_csv(seeds_dir / "fake_data_transactions.csv")
    for _, row in transactions_df.iterrows():
        transaction = TransactionsModel(
            id=row["id"],
            position_id=row["position_id"],
            type=row["type"],
            quantity=row["quantity"],
            price_brl=row["price_brl"],
            executed_at=datetime.fromisoformat(row["executed_at"]),
            created_at=datetime.fromisoformat(row["created_at"])
        )
        session.add(transaction)
    
    await session.commit()


async def override_get_session():
    async with TestingSessionlocal() as session:
        yield session


@pytest_asyncio.fixture
async def populated_db():
    """Create database and populate with seed data"""
    async with engine_test.begin() as conn:
        await conn.run_sync(settings.DBBaseModel.metadata.create_all)
    
    async with TestingSessionlocal() as session:
        await load_seed_data(session)
    
    yield
    
    async with engine_test.begin() as conn:
        await conn.run_sync(settings.DBBaseModel.metadata.drop_all)
    await engine_test.dispose()


@pytest_asyncio.fixture
async def client(create_db):
    app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client_with_data(populated_db):
    """Client with pre-populated database"""
    app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
