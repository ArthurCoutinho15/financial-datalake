from datetime import datetime
from sqlalchemy import Column, DateTime, String
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.core.configs import settings


class ClientsModel(settings.DBBaseModel):
    __tablename__ = "clients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(150), nullable=False)
    email = Column(String(200), nullable=False, unique=True, index=True)
    cpf = Column(String(14), nullable=False, unique=True, index=True)
    phone = Column(String(20), nullable=False)
    city = Column(String(200), nullable=False)
    state = Column(String(2), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    portfolios = relationship(
        "PortfoliosModel",
        back_populates="client",
        foreign_keys="PortfoliosModel.client_id",
    )
