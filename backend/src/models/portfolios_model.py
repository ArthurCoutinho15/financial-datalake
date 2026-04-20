from datetime import datetime
from sqlalchemy import Column, DateTime, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.core.configs import settings


class PortfoliosModel(settings.DBBaseModel):
    __tablename__ = "portfolios"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False)

    name = Column(String(150), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    client = relationship(
        "ClientsModel", foreign_keys=[client_id], back_populates="portfolios"
    )
    positions = relationship(
        "PositionsModel", back_populates="portfolio", cascade="all, delete-orphan"
    )
