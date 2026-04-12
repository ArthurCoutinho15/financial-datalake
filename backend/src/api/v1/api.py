from fastapi import APIRouter

from src.api.v1.routes import analytics

api_router = APIRouter()

api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])