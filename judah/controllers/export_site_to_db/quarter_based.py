"""Module containing the controller class for getting data from the quarter-based export site to the database"""

from typing import Type, Optional, Tuple

from ..base.quarter_based import QuarterBasedBaseController
from ...destinations.database.model import DatabaseBaseModel
from ...sources.export_site.quarter_based import QuarterBasedExportSiteSource
from ...utils.dates import get_default_historical_start_quarter_and_year


class QuarterBasedExportSiteToDatabaseController(QuarterBasedBaseController):
    """
    Class for the controller that downloads data from the quarter-based export site
    and saves it in a database
    """
    _source: QuarterBasedExportSiteSource

    destination_model_class: Type[DatabaseBaseModel]
    source_class: Type[QuarterBasedExportSiteSource]
    start_quarter_and_year: Optional[Tuple[int, int]] = get_default_historical_start_quarter_and_year()