from uuid import UUID

from fastapi import APIRouter, status, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.schemas.portfolios_schema import (
    PortfolioSchema,
    PortfolioCreateSchema,
    PortfolioUpdateSchema,
)
from src.schemas.pagination_schema import PaginatedResponse
from src.services.portfolios_service import PortfoliosService


router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PortfolioSchema)
async def post_portfolios(
    portfolio: PortfolioCreateSchema, db: AsyncSession = Depends(get_session)
):
    portfolios_service = PortfoliosService(db)

    return await portfolios_service.post_portfolio(portfolio)


@router.get(
    "/",
    status_code=status.HTTP_200_OK,
    response_model=PaginatedResponse[PortfolioSchema],
)
async def get_portfolios(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        10, ge=1, le=100, description="Number of records to return (max 100)"
    ),
    db: AsyncSession = Depends(get_session),
):
    portfolios_service = PortfoliosService(db)
    portfolios, total = await portfolios_service.get_portfolios(skip=skip, limit=limit)

    return PaginatedResponse[PortfolioSchema](
        data=portfolios, total=total, skip=skip, limit=limit
    )


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
