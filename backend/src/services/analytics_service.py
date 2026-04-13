from typing import List

from fastapi import APIRouter, status, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from src.models.analytics_positions_model import AnalyticsPositionsModel


class AnalyticsService:
    def __init__(self, db: AsyncSession):
        self.db = db
        
    
    async def get_analytics_data(self) -> List[dict]:
        async with self.db as session:
            query = select(AnalyticsPositionsModel)
            results = await session.execute(query)
            
            data = results.scalars().unique().all()
            
            return data
            