from typing import Optional, List
from pydantic import BaseModel, EmailStr
from datetime import datetime
from uuid import UUID


class ClientsSchema(BaseModel):
    id: Optional[UUID] = None
    name: str 
    email: EmailStr
    cpf: str
    phone: str
    city: str 
    state: str 
    
    class Config:
        orm_mode = True