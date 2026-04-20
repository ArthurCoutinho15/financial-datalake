from datetime import datetime
from sqlalchemy import Column, DateTime, String, ForeignKey, DECIMAL
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.core.configs import settings


class PositionsModel(settings.DBBaseModel):
    __tablename__ = "positions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    portfolio_id = Column(
        UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False
    )

    ticker = Column(String(150), nullable=False)
    asset_type = Column(String(150), nullable=False)
    quantity = Column(DECIMAL(18, 6), nullable=False)
    avg_price_brl = Column(DECIMAL(18, 6), nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now,  onupdate=datetime.now)

    portfolio = relationship("PortfoliosModel", back_populates="positions")
    transactions = relationship(
        "TransactionsModel", back_populates="position", cascade="all, delete-orphan"
    )
