from typing import List
from uuid import UUID

from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.schemas.transactions_schema import (
    TransactionsSchema,
    TransactionsCreateSchema,
    TransactionsUpdateSchema,
)
from src.services.transactions_service import TransactionsService


router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=TransactionsSchema)
async def post_transactions(
    transaction: TransactionsCreateSchema, db: AsyncSession = Depends(get_session)
):
    transactions_service = TransactionsService(db)

    return await transactions_service.post_transaction(transaction)


@router.get("/", status_code=status.HTTP_200_OK, response_model=List[TransactionsSchema])
async def get_transactions(db: AsyncSession = Depends(get_session)):
    transactions_service = TransactionsService(db)

    return await transactions_service.get_transactions()


@router.get(
    "/{transaction_id}", status_code=status.HTTP_200_OK, response_model=TransactionsSchema
)
async def get_transaction(transaction_id: UUID, db: AsyncSession = Depends(get_session)):
    transactions_service = TransactionsService(db)

    return await transactions_service.get_transaction(transaction_id)


@router.put(
    "/{transaction_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TransactionsSchema,
)
async def put_transaction(
    transaction_id: UUID,
    transaction: TransactionsUpdateSchema,
    db: AsyncSession = Depends(get_session),
):
    transactions_service = TransactionsService(db)

    return await transactions_service.put_transaction(transaction_id, transaction)


@router.delete("/{transaction_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_transaction(transaction_id: UUID, db: AsyncSession = Depends(get_session)):
    transactions_service = TransactionsService(db)

    return await transactions_service.delete_transaction(transaction_id)
