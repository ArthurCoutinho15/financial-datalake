from sqlalchemy import (
    Column,
    Float,
    Date,
    Text,
    UUID,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import relationship
import uuid

from src.core.configs import settings


class AnalyticsPositionsModel(settings.DBBaseModel):
    __tablename__ = "fct_positions"
    __table_args__ = (
        PrimaryKeyConstraint("client_id", "ticker", "dt_reference"),
    )
    
    dt_reference = Column(Date, nullable=True)

    client_id = Column(UUID(as_uuid=True), nullable=True)

    net_quantity = Column(Float, nullable=True)
    net_invested = Column(Float, nullable=True)
    current_price_usd = Column(Float, nullable=True)
    usd_brl = Column(Float, nullable=True)
    fx_price_brl = Column(Float, nullable=True)
    avg_price = Column(Float, nullable=True)
    position_value_brl = Column(Float, nullable=True)
    pnl = Column(Float, nullable=True)
    return_pct = Column(Float, nullable=True)

    ticker = Column(Text, nullable=True)