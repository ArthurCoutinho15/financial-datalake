from typing import List

from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.services.analytics_service import AnalyticsService

router = APIRouter()


@router.get("/", status_code=status.HTTP_200_OK)
async def get_analytics_data(db: AsyncSession = Depends(get_session)):
    analytics_service = AnalyticsService(db=db)

    positions_data = await analytics_service.get_analytics_data()

    return positions_data
