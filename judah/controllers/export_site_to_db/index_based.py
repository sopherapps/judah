"""
Module with base class for index based export site sources whereby
files are downloaded in order basing on their position or index in a file list.
"""
from typing import Type

from judah.controllers.base import BaseController
from judah.destinations.database.model import DatabaseBaseModel
from judah.sources.export_site.index_based import IndexBasedExportSiteSource


class IndexBasedExportSiteToDBController(BaseController):
    """
    Class for the controller that downloads data from a index-based export site
    and saves it in a database
    """
    _source: IndexBasedExportSiteSource

    destination_model_class: Type[DatabaseBaseModel]
    source_class: Type[IndexBasedExportSiteSource]
