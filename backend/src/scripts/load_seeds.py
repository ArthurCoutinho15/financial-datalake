"""Script para carregar dados de seed no banco de dados"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.configs import settings
from src.models.clients_model import ClientsModel
from src.models.portfolios_model import PortfoliosModel
from src.models.positions_model import PositionsModel
from src.models.transactions_model import TransactionsModel


async def load_seeds():
    """Load seed data from CSV files into the database"""
    
    # Create engine using the same database as the app
    engine = create_async_engine(
        settings.DB_URL,
        echo=True
    )
    
    AsyncSessionLocal = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    seeds_dir = Path(__file__).parent.parent.parent / "tests" / "seeds"
    
    if not seeds_dir.exists():
        print(f"❌ Seeds directory not found: {seeds_dir}")
        return
    
    async with AsyncSessionLocal() as session:
        try:
            # Load clients
            print("\n📥 Loading clients...")
            clients_file = seeds_dir / "fake_data_clients.csv"
            if clients_file.exists():
                clients_df = pd.read_csv(clients_file)
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
                print(f"✅ Loaded {len(clients_df)} clients")
            
            # Load portfolios
            print("\n📥 Loading portfolios...")
            portfolios_file = seeds_dir / "fake_data_portfolios.csv"
            if portfolios_file.exists():
                portfolios_df = pd.read_csv(portfolios_file)
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
                print(f"✅ Loaded {len(portfolios_df)} portfolios")
            
            # Load positions
            print("\n📥 Loading positions...")
            positions_file = seeds_dir / "fake_data_positions.csv"
            if positions_file.exists():
                positions_df = pd.read_csv(positions_file)
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
                print(f"✅ Loaded {len(positions_df)} positions")
            
            # Load transactions
            print("\n📥 Loading transactions...")
            transactions_file = seeds_dir / "fake_data_transactions.csv"
            if transactions_file.exists():
                transactions_df = pd.read_csv(transactions_file)
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
                await session.flush()
                print(f"✅ Loaded {len(transactions_df)} transactions")
            
            # Commit all data
            await session.commit()
            print("\n✨ All seed data loaded successfully!")
            
        except Exception as e:
            await session.rollback()
            print(f"\n❌ Error loading seeds: {e}")
            raise
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(load_seeds())
