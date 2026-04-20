from fastapi import APIRouter

from src.api.v1.routes import analytics, clients, portfolios, positions, transactions

api_router = APIRouter()

api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(clients.router, prefix="/clients", tags=["clients"])
api_router.include_router(portfolios.router, prefix="/portfolios", tags=["portfolios"])
api_router.include_router(positions.router, prefix="/positions", tags=["positions"])
api_router.include_router(transactions.router, prefix="/transactions", tags=["transactions"])