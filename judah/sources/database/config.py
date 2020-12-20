"""A few configuration classes for the database source"""
from pydantic import BaseModel


class DatabaseSourceDataModel(BaseModel):
    """The base model for data models of the Database source"""

    class Config:
        orm_mode = True
