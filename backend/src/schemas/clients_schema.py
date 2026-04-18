from typing import Optional
from pydantic import BaseModel, EmailStr
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


class ClientsCreateSchema(BaseModel):
    name: str
    email: EmailStr
    cpf: str
    phone: str
    city: str
    state: str

    class Config:
        orm_mode = True


class ClientsUpdateSchema(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    cpf: Optional[str] = None
    phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None

    class Config:
        orm_mode = True
