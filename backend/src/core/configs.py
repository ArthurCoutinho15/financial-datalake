import os
from dotenv import load_dotenv

from typing import ClassVar
from pydantic_settings import BaseSettings
from sqlalchemy.orm import declarative_base


load_dotenv()


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    DB_URL: str = (
        f"postgresql+asyncpg://{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASS', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'financial_db')}"
    )

    DB_URL_SYNC: str = (
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASS', 'postgres')}@{os.getenv('DB_HOST_LOCAL', 'localhost')}:"
        f"{os.getenv('DB_PORT_ALEMBIC', '5432')}/{os.getenv('DB_NAME', 'financial_db')}"
    )
    DBBaseModel: ClassVar = declarative_base()
    
    class Config:
        case_sensitive = True

settings: Settings = Settings()
    
