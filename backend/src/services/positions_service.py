from typing import List
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from fastapi import HTTPException, status, Response

from src.models.positions_model import PositionsModel
from src.schemas.positions_schema import (
    PositionsCreateSchema,
    PositionsSchema,
    PositionsUpdateSchema,
)


class PositionsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def post_position(self, position: PositionsCreateSchema) -> PositionsModel:
        new_position = PositionsModel(**position.model_dump())

        self.db.add(new_position)
        await self.db.commit()
        await self.db.refresh(new_position)

        return new_position

    async def get_positions(self) -> List[PositionsModel]:
        query = select(PositionsModel)
        results = await self.db.execute(query)

        positions = results.scalars().unique().all()

        return positions

    async def get_position(self, position_id: UUID) -> PositionsModel:
        query = select(PositionsModel).where(PositionsModel.id == position_id)
        result = await self.db.execute(query)

        position = result.scalars().unique().one_or_none()

        if not position:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Position not found"
            )

        return position

    async def put_position(
        self, position_id: UUID, position: PositionsUpdateSchema
    ) -> PositionsModel:
        query = select(PositionsModel).where(PositionsModel.id == position_id)
        result = await self.db.execute(query)

        position_up = result.scalars().unique().one_or_none()

        if not position_up:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Position not found"
            )

        update_data = position.model_dump(exclude_unset=True)

        for key, value in update_data.items():
            setattr(position_up, key, value)

        await self.db.commit()
        await self.db.refresh(position_up)

        return position_up

    async def delete_position(self, position_id: UUID) -> None:
        query = select(PositionsModel).where(PositionsModel.id == position_id)
        result = await self.db.execute(query)

        position_del = result.scalars().unique().one_or_none()

        if not position_del:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Position not found"
            )

        await self.db.delete(position_del)
        await self.db.commit()

        return Response(status_code=status.HTTP_204_NO_CONTENT)
