"""Module containing the controller class for getting data from the rest api to the database"""
from typing import Type

from ..base.date_based import DateBasedBaseController
from ...destinations.database.model import DatabaseBaseModel
from ...sources.rest_api.date_based import DateBasedRestAPISource


class DateBasedRestAPIToDatabaseController(DateBasedBaseController):
    """
    Class for the controller that gets data from the REST API
    and saves it in a database
    """
    _source: DateBasedRestAPISource

    destination_model_class: Type[DatabaseBaseModel]
    source_class: Type[DateBasedRestAPISource]
