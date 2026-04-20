from typing import Optional
from datetime import datetime
from enum import Enum

from pydantic import BaseModel
from uuid import UUID


class TransactionTypeEnum(str, Enum):
    buy = "buy"
    sell = "sell"


class TransactionsSchema(BaseModel):
    id: Optional[UUID] = None
    position_id: Optional[UUID] = None
    type: TransactionTypeEnum
    quantity: float
    price_brl: float
    executed_at: datetime
    created_at: datetime

    class Config:
        orm_mode = True


class TransactionsCreateSchema(BaseModel):
    position_id: UUID
    type: TransactionTypeEnum
    quantity: float
    price_brl: float
    executed_at: datetime

    class Config:
        orm_mode = True


class TransactionsUpdateSchema(BaseModel):
    type: TransactionTypeEnum
    quantity: float
    price_brl: float
    executed_at: datetime

    class Config:
        orm_mode = True
