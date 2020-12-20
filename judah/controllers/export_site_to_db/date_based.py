"""Module containing the controller class for getting data from the date based export site to the database"""

from datetime import date
from typing import Type, Optional

from ..base.date_based import DateBasedBaseController
from ...destinations.database.model import DatabaseBaseModel
from ...sources.export_site.date_based import DateBasedExportSiteSource
from ...utils.dates import get_default_historical_start_date


class DateBasedExportSiteToDatabaseController(DateBasedBaseController):
    """
    Class for the controller that downloads data from the date-based export site
    and saves it in a database
    """
    _source: DateBasedExportSiteSource

    destination_model_class: Type[DatabaseBaseModel]
    source_class: Type[DateBasedExportSiteSource]
    start_date: Optional[date] = get_default_historical_start_date()
