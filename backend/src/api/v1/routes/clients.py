from typing import List

from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.schemas.clients_schema import ClientsSchema
from src.services.clients_service import ClientsService

router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=ClientsSchema)
async def post_client(client: ClientsSchema, db: AsyncSession = Depends(get_session)):
    client_service = ClientsService(db)

    return await client_service.post_client(client)

@router.get("/", status_code=status.HTTP_200_OK, response_model=List[ClientsSchema])
async def get_clients(db: AsyncSession = Depends(get_session)):
    client_service = ClientsService(db)
    
    return await client_service.get_clients()
    
    