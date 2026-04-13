from datetime import datetime
from enum import Enum as BaseEnum

from sqlalchemy import Column, DateTime, String, ForeignKey, DECIMAL, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.core.configs import settings


class TransactionTypeEnum(str, BaseEnum):
    buy = "buy"
    sell = "sell"


class TransactionsModel(settings.DBBaseModel):
    __tablename__ = "transactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    position_id = Column(UUID(as_uuid=True), ForeignKey("positions.id"), nullable=False)

    type = Column(
        Enum(TransactionTypeEnum, name="transaction_type_enum"), nullable=False
    )
    quantity = Column(DECIMAL(18, 6), nullable=False)
    price_brl = Column(DECIMAL(18, 6), nullable=False)

    executed_at = Column(DateTime, nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.now)

    position = relationship("PositionsModel", back_populates="transactions")
