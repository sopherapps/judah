"""Module containing the controller class for getting data from database to the database"""
from typing import Type

from ..base.date_based import DateBasedBaseController
from ...destinations.database.model import DatabaseBaseModel
from ...sources.database.date_based import DateBasedDatabaseSource


class DateBasedDatabaseToDatabaseController(DateBasedBaseController):
    """
    Class for the controller that gets data from the database
    and saves it in a database
    """
    _source: DateBasedDatabaseSource

    destination_model_class: Type[DatabaseBaseModel]
    source_class: Type[DateBasedDatabaseSource]
