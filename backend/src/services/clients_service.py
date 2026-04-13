from typing import List, Optional, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.future import select

from src.models.clients_model import ClientsModel
from src.schemas.clients_schema import ClientsSchema


class ClientsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def post_client(self, client: ClientsSchema) -> ClientsModel:
        new_client = ClientsModel(**client.model_dump())

        self.db.add(new_client)
        await self.db.commit()

        return new_client

    async def get_clients(self) -> List[ClientsModel]:
        query = select(ClientsModel)
        results = await self.db.execute(query)

        clients = results.scalars().unique().all()

        return clients
