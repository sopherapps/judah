"""Configurations for the database package"""
from pydantic import BaseModel


class DatabaseConnectionConfig(BaseModel):
    db_uri: str
    pool_size: int = 5
    reflect: bool = True
    autocommit: bool = False
    pool_timeout: int = 60
