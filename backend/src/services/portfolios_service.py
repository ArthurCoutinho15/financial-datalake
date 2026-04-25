from typing import List, Tuple
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func

from fastapi import HTTPException, status, Response

from src.models.portfolios_model import PortfoliosModel
from src.schemas.portfolios_schema import PortfolioCreateSchema, PortfolioUpdateSchema


class PortfoliosService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def post_portfolio(self, portfolio: PortfolioCreateSchema) -> PortfoliosModel:
        new_portfolio = PortfoliosModel(**portfolio.model_dump())

        self.db.add(new_portfolio)
        await self.db.commit()
        await self.db.refresh(new_portfolio)

        return new_portfolio

    # async def get_portfolios(self) -> List[PortfoliosModel]:
    #     query = select(PortfoliosModel)
    #     results = await self.db.execute(query)

    #     portfolios = results.scalars().unique().all()

    #     return portfolios

    async def get_portfolios(
        self, skip: int = 0, limit: int = 10
    ) -> Tuple[List[PortfoliosModel], int]:
        """Get portfolios with pagination"""
        # Get total count
        count_query = select(func.count(PortfoliosModel.id))
        count_result = await self.db.execute(count_query)
        total = count_result.scalar()

        # Get paginated results
        query = (
            select(PortfoliosModel)
            .order_by(PortfoliosModel.created_at)
            .offset(skip)
            .limit(limit)
        )
        results = await self.db.execute(query)
        portfolios = results.scalars().unique().all()

        return portfolios, total

    async def get_portfolio(self, portfolio_id: UUID) -> PortfoliosModel:
        query = select(PortfoliosModel).where(PortfoliosModel.id == portfolio_id)
        result = await self.db.execute(query)

        portfolio = result.scalars().unique().one_or_none()

        if not portfolio:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )

        return portfolio

    async def put_portfolio(
        self, portfolio_id: UUID, portfolio: PortfolioUpdateSchema
    ) -> PortfoliosModel:
        query = select(PortfoliosModel).where(PortfoliosModel.id == portfolio_id)
        result = await self.db.execute(query)

        portfolio_up = result.scalars().unique().one_or_none()

        if not portfolio_up:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )

        update_data = portfolio.model_dump(exclude_unset=True)

        for key, value in update_data.items():
            setattr(portfolio_up, key, value)

        await self.db.commit()
        await self.db.refresh(portfolio_up)

        return portfolio_up

    async def delete_portfolio(self, portfolio_id: UUID) -> None:
        query = select(PortfoliosModel).where(PortfoliosModel.id == portfolio_id)
        result = await self.db.execute(query)

        portfolio_del = result.scalars().unique().one_or_none()

        if not portfolio_del:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )

        await self.db.delete(portfolio_del)
        await self.db.commit()

        return Response(status_code=status.HTTP_204_NO_CONTENT)
