from typing import List
from uuid import UUID

from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.deps import get_session

from src.schemas.clients_schema import (
    ClientsSchema,
    ClientsUpdateSchema,
    ClientsCreateSchema,
)
from src.services.clients_service import ClientsService

router = APIRouter()


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=ClientsSchema)
async def post_client(
    client: ClientsCreateSchema, db: AsyncSession = Depends(get_session)
):
    client_service = ClientsService(db)

    return await client_service.post_client(client)


@router.get("/", status_code=status.HTTP_200_OK, response_model=List[ClientsSchema])
async def get_clients(db: AsyncSession = Depends(get_session)):
    client_service = ClientsService(db)

    return await client_service.get_clients()


@router.get(
    "/{client_id}", status_code=status.HTTP_200_OK, response_model=ClientsSchema
)
async def get_client(client_id: UUID, db: AsyncSession = Depends(get_session)):
    client_service = ClientsService(db)

    return await client_service.get_client(client_id)


@router.put(
    "/{client_id}", status_code=status.HTTP_202_ACCEPTED, response_model=ClientsSchema
)
async def put_client(
    client_id: UUID,
    client: ClientsUpdateSchema,
    db: AsyncSession = Depends(get_session),
):
    client_service = ClientsService(db)

    return await client_service.put_client(client_id, client)


@router.delete("/{client_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_client(client_id: UUID, db: AsyncSession = Depends(get_session)):
    client_service = ClientsService(db)

    return await client_service.delete_client(client_id)
