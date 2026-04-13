import os
from dotenv import load_dotenv

from typing import ClassVar
from pydantic_settings import BaseSettings
from sqlalchemy.orm import declarative_base


load_dotenv()


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    DB_URL: str = (
        f"postgresql+asyncpg://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    DB_URL_SYNC: str = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASS')}@{os.getenv('DB_HOST_LOCAL')}:"
        f"{os.getenv('DB_PORT_ALEMBIC')}/{os.getenv('DB_NAME')}"
    )
    DBBaseModel: ClassVar = declarative_base()
    
    class Config:
        case_sensitive = True

settings: Settings = Settings()
    
