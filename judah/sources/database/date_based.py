"""Module containing configurations for the date-based database data source"""

from datetime import date
from typing import Dict, Any, Optional, Type, Generator, Iterator

from sqlalchemy import text

from .config import DatabaseSourceDataModel
from ..base.date_based import DateBasedBaseSource
from ...destinations.database.config import DatabaseConnectionConfig
from ...destinations.database.connection import DatabaseConnection


class DateBasedDatabaseSource(DateBasedBaseSource):
    """Base class for the database source that depends on dates"""
    db_configuration: DatabaseConnectionConfig
    sql_query: str
    batch_size: int = 200
    data_model_class: Type[DatabaseSourceDataModel]

    def _get_params(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[Any, Any]:
        """An overridable method to return params dict to use in the SQL execute"""
        return {}

    def _query_data_source(self, start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
        """Queries a given start and end date and returns an iterator with data records"""
        params = self._get_params(start_date=start_date, end_date=end_date)

        with DatabaseConnection.get_db_connection(db_connection_config=self.db_configuration) as db_connection:
            result_proxy = db_connection.connection_engine.execution_options(stream_results=True).execute(
                text(self.sql_query), **params)

            while 'batch is not empty':
                # for memory efficiency, I have decided to query in batches of default size 200
                batch = result_proxy.fetchmany(size=self.batch_size)

                if not batch:
                    break

                yield from [self.data_model_class.from_orm(record).dict() for record in batch]

            result_proxy.close()

        yield from []
