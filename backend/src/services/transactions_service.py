from typing import List
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from fastapi import HTTPException, status, Response

from src.models.transactions_model import TransactionsModel
from src.schemas.transactions_schema import TransactionsCreateSchema, TransactionsSchema, TransactionsUpdateSchema


class TransactionsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def post_transaction(self, transaction: TransactionsCreateSchema) -> TransactionsModel:
        new_transaction = TransactionsModel(**transaction.model_dump())

        self.db.add(new_transaction)
        await self.db.commit()
        await self.db.refresh(new_transaction)

        return new_transaction

    async def get_transactions(self) -> List[TransactionsModel]:
        query = select(TransactionsModel)
        results = await self.db.execute(query)

        transactions = results.scalars().unique().all()

        return transactions

    async def get_transaction(self, transaction_id: UUID) -> TransactionsModel:
        query = select(TransactionsModel).where(TransactionsModel.id == transaction_id)
        result = await self.db.execute(query)

        transaction = result.scalars().unique().one_or_none()

        if not transaction:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found"
            )

        return transaction

    async def put_transaction(self, transaction_id: UUID, transaction: TransactionsUpdateSchema) -> TransactionsModel:
        query = select(TransactionsModel).where(TransactionsModel.id == transaction_id)
        result = await self.db.execute(query)

        transaction_up = result.scalars().unique().one_or_none()

        if not transaction_up:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found"
            )

        update_data = transaction.model_dump(exclude_unset=True)

        for key, value in update_data.items():
            setattr(transaction_up, key, value)

        await self.db.commit()
        await self.db.refresh(transaction_up)

        return transaction_up

    async def delete_transaction(self, transaction_id: UUID) -> None:
        query = select(TransactionsModel).where(TransactionsModel.id == transaction_id)
        result = await self.db.execute(query)

        transaction_del = result.scalars().unique().one_or_none()

        if not transaction_del:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found"
            )

        await self.db.delete(transaction_del)
        await self.db.commit()

        return Response(status_code=status.HTTP_204_NO_CONTENT)
