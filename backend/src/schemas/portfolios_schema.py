from typing import Optional
from datetime import datetime 

from pydantic import BaseModel, EmailStr
from uuid import UUID
from enum import Enum


class PortfolioNameEnum(str, Enum):
    conservador = "Conservador"
    internacional = "Internacional"
    previdencia = "Previdência"
    moderado = "Moderado"
    renda_variavel = "Renda Variável"
    arrojado = "Arrojado"


class PortfolioSchema(BaseModel):
    id: Optional[UUID] = None
    client_id: UUID
    name: PortfolioNameEnum
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class PortfolioCreateSchema(BaseModel):
    client_id: UUID
    name: PortfolioNameEnum

    class Config:
        orm_mode = True
        
class PortfolioUpdateSchema(BaseModel):
    name: Optional[PortfolioNameEnum] = None

    class Config:
        orm_mode = True
