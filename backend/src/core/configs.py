import os
from dotenv import load_dotenv

from typing import ClassVar
from pydantic_settings import BaseSettings
from sqlalchemy.orm import declarative_base


load_dotenv()


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    
    ENV: str = os.getenv("ENV", "dev")
    
    DB_USER: str
    DB_PASS: str
    DB_NAME: str
    DB_PORT: str
    DB_PORT_LOCAL: str = os.getenv("DB_PORT_LOCAL", "5433")

    DB_HOST: str
    DB_HOST_LOCAL: str
    
    @property
    def DB_URL(self):
        if self.ENV == "docker":
            host = self.DB_HOST
            port = self.DB_PORT
        else:
            host = self.DB_HOST_LOCAL
            port = self.DB_PORT_LOCAL

        return (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}"
            f"@{host}:{port}/{self.DB_NAME}"
        )

    @property
    def DB_URL_SYNC(self):
        if self.ENV == "docker":
            host = self.DB_HOST
            port = self.DB_PORT
        else:
            host = self.DB_HOST_LOCAL
            port = self.DB_PORT_LOCAL
        
        return (
            f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASS}"
            f"@{host}:{port}/{self.DB_NAME}"
        )
    
    DBBaseModel: ClassVar = declarative_base()
    
    class Config:
        case_sensitive = True

settings: Settings = Settings()
    
