from typing import Optional
from datetime import datetime 

from pydantic import BaseModel, EmailStr
from uuid import UUID


class PositionsSchema(BaseModel):
    id: Optional[UUID] = None
    portfolio_id: Optional[UUID] = None 
    ticker: str 
    asset_type: str
    quantity: float 
    avg_price_brl: float 
    created_at: datetime 
    updated_at: datetime 
    
    class Config:
        orm_mode = True
        
class PositionsCreateSchema(BaseModel):
    portfolio_id: UUID  
    ticker: str 
    asset_type: str
    quantity: float 
    avg_price_brl: float 
    
    class Config:
        orm_mode = True
        
class PositionsUpdateSchema(BaseModel):
    ticker: Optional[str] = None 
    asset_type: Optional[str] = None
    quantity: Optional[float] = None 
    avg_price_brl: Optional[float] = None 
    
    class Config:
        orm_mode = True