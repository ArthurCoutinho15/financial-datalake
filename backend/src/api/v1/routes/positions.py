from typing import List
from uuid import UUID

from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.schemas.positions_schema import (
    PositionsSchema,
    PositionsCreateSchema,
    PositionsUpdateSchema,
)
from src.services.positions_service import PositionsService


router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PositionsSchema)
async def post_positions(
    position: PositionsCreateSchema, db: AsyncSession = Depends(get_session)
):
    positions_service = PositionsService(db)

    return await positions_service.post_position(position)


@router.get("/", status_code=status.HTTP_200_OK, response_model=List[PositionsSchema])
async def get_positions(db: AsyncSession = Depends(get_session)):
    positions_service = PositionsService(db)

    return await positions_service.get_positions()


@router.get(
    "/{position_id}", status_code=status.HTTP_200_OK, response_model=PositionsSchema
)
async def get_position(position_id: UUID, db: AsyncSession = Depends(get_session)):
    positions_service = PositionsService(db)

    return await positions_service.get_position(position_id)


@router.put(
    "/{position_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PositionsSchema,
)
async def put_position(
    position_id: UUID,
    position: PositionsUpdateSchema,
    db: AsyncSession = Depends(get_session),
):
    positions_service = PositionsService(db)

    return await positions_service.put_position(position_id, position)


@router.delete("/{position_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_position(position_id: UUID, db: AsyncSession = Depends(get_session)):
    positions_service = PositionsService(db)

    return await positions_service.delete_position(position_id)
