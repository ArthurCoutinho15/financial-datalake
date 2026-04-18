from typing import List
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from fastapi import HTTPException, status, Response

from src.models.clients_model import ClientsModel
from src.schemas.clients_schema import ClientsUpdateSchema, ClientsCreateSchema


class ClientsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def post_client(self, client: ClientsCreateSchema) -> ClientsModel:
        new_client = ClientsModel(**client.model_dump())

        self.db.add(new_client)
        await self.db.commit()
        await self.db.refresh(new_client)

        return new_client

    async def get_clients(self) -> List[ClientsModel]:
        query = select(ClientsModel)
        results = await self.db.execute(query)

        clients = results.scalars().unique().all()

        return clients

    async def get_client(self, client_id: UUID) -> ClientsModel:
        query = select(ClientsModel).where(ClientsModel.id == client_id)
        result = await self.db.execute(query)

        client = result.scalars().unique().one_or_none()

        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Client not found"
            )

        return client

    async def put_client(
        self, client_id: UUID, client: ClientsUpdateSchema
    ) -> ClientsModel:
        query = select(ClientsModel).where(ClientsModel.id == client_id)
        result = await self.db.execute(query)

        client_up = result.scalars().unique().one_or_none()

        if not client_up:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Client not found"
            )

        update_data = client.model_dump(exclude_unset=True)

        for key, value in update_data.items():
            setattr(client_up, key, value)

        await self.db.commit()
        await self.db.refresh(client_up)

        return client_up

    async def delete_client(self, client_id: UUID) -> ClientsModel:
        query = select(ClientsModel).where(ClientsModel.id == client_id)
        result = await self.db.execute(query)

        client_del = result.scalars().unique().one_or_none()

        if not client_del:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Client not found"
            )

        await self.db.delete(client_del)
        await self.db.commit()

        return Response(status_code=status.HTTP_204_NO_CONTENT)
