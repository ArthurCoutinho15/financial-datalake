from typing import List
from uuid import UUID

from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.schemas.portfolios_schema import (
    PortfolioSchema,
    PortfolioCreateSchema,
    PortfolioUpdateSchema,
)
from src.services.portfolios_service import PortfoliosService


router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PortfolioSchema)
async def post_portfolios(
    portfolio: PortfolioCreateSchema, db: AsyncSession = Depends(get_session)
):
    portfolios_service = PortfoliosService(db)

    return await portfolios_service.post_portfolio(portfolio)


@router.get("/", status_code=status.HTTP_200_OK, response_model=List[PortfolioSchema])
async def get_portfolios(db: AsyncSession = Depends(get_session)):
    portfolios_service = PortfoliosService(db)

    return await portfolios_service.get_portfolios()


@router.get(
    "/{portfolio_id}", status_code=status.HTTP_200_OK, response_model=PortfolioSchema
)
async def get_portfolio(portfolio_id: UUID, db: AsyncSession = Depends(get_session)):
    portfolios_service = PortfoliosService(db)

    return await portfolios_service.get_portfolio(portfolio_id)


@router.put(
    "/{portfolio_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PortfolioSchema,
)
async def put_portfolio(
    portfolio_id: UUID,
    portfolio: PortfolioUpdateSchema,
    db: AsyncSession = Depends(get_session),
):
    portfolios_service = PortfoliosService(db)

    return await portfolios_service.put_portfolio(portfolio_id, portfolio)


@router.delete("/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_portfolio(portfolio_id: UUID, db: AsyncSession = Depends(get_session)):
    portfolios_service = PortfoliosService(db)

    return await portfolios_service.delete_portfolio(portfolio_id)
