from fastapi import APIRouter

from src.api.v1.routes import analytics, clients

api_router = APIRouter()

api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(clients.router, prefix="/clients", tags=["clients"])