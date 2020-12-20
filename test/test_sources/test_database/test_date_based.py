"""Module with tests for the date based database source source"""
import os
from collections import Iterator
from datetime import date
from typing import Dict, Type, Optional, List, Any
from unittest import TestCase, main

from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as orm

from judah.destinations.database.config import DatabaseConnectionConfig
from judah.destinations.database.connection import DatabaseConnection
from judah.destinations.database.model import DatabaseBaseModel
from judah.sources.database.config import DatabaseSourceDataModel
from judah.sources.database.date_based import DateBasedDatabaseSource
from test.utils import delete_tables_from_database, create_tables_in_database

_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

load_dotenv(os.path.join(_ROOT_PATH, '.env'))

_TEST_DB_URI = os.environ.get('TEST_POSTGRES_DB_URI')

_TEST_DB_BASE = declarative_base()
_TABLE_NAME = 'dummy_source'
_SCHEMA_NAME = 'test_schema_for_source'


class TestModel(DatabaseBaseModel, _TEST_DB_BASE):
    """Test database model"""
    __tablename__ = _TABLE_NAME
    __table_args__: Dict = {'schema': _SCHEMA_NAME}
    _db_configuration: DatabaseConnectionConfig = DatabaseConnectionConfig(db_uri=_TEST_DB_URI)
    _base_declarative_class: Type[declarative_base()] = _TEST_DB_BASE
    _datetime_fields: Optional[List[str]] = ["Date"]

    Date = orm.Column(orm.Date, primary_key=True)
    number = orm.Column(orm.Integer, primary_key=True)
    Capacity = orm.Column(orm.Integer)
    Price = orm.Column(orm.Integer)


class TestDatabaseSourceDataModel(DatabaseSourceDataModel):
    Date: date
    number: int
    Capacity: int
    Price: int


class TestDateBasedDatabaseSource(TestCase):
    """Tests for the DateBasedDatabaseSource"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.data = [
            {"Date": date(year=2020, month=3, day=9), "number": 1, "Capacity": 16616, "Price": 67},
            {"Date": date(year=2020, month=3, day=12), "number": 2, "Capacity": 16516, "Price": 567},
            {"Date": date(year=2020, month=3, day=10), "number": 3, "Capacity": 16616, "Price": 637},
            {"Date": date(year=2020, month=3, day=9), "number": 4, "Capacity": 16620, "Price": 617},
        ]
        try:
            delete_tables_from_database(db_configuration=TestModel._db_configuration,
                                        table_name=_TABLE_NAME, schema_name=_SCHEMA_NAME)
        except Exception:
            pass

    def load_database(self):
        """Loads the database with the self.data"""
        create_tables_in_database(db_configuration=TestModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            for datum in self.data:
                record = TestModel(**datum)
                db_connection.db_session.add(record)

            db_connection.db_session.commit()

    def test_get_with_params(self):
        """Makes an SQL query with the params from the _get_params method and returns a generator (iterator)"""
        self.load_database()

        dummy_params = {'number': 3}

        class DateBasedDatabaseSourceWithParams(DateBasedDatabaseSource):
            sql_query: str = f"""
            SELECT * FROM {_SCHEMA_NAME}.{_TABLE_NAME}
            WHERE "number" = :number
            """
            name: str = 'sample'
            db_configuration: DatabaseConnectionConfig = TestModel._db_configuration
            data_model_class: Type[DatabaseSourceDataModel] = TestDatabaseSourceDataModel

            def _get_params(self, start_date: Optional[date] = None, end_date: Optional = None) -> Dict[Any, Any]:
                return dummy_params

        source = DateBasedDatabaseSourceWithParams()
        response = source.get()

        self.assertIsInstance(response, Iterator)

        expected_data = [self.data[2]]
        received_data = list(response)

        self.assertListEqual(expected_data, received_data)

    def test_get_with_default_params(self):
        """Makes an SQL query with the default params of {} and returns an appropriate generator (iterator)"""
        self.load_database()

        class DateBasedDatabaseSourceWithParams(DateBasedDatabaseSource):
            sql_query: str = f"""
            SELECT * FROM {_SCHEMA_NAME}.{_TABLE_NAME}
            WHERE "number" IN (2, 4)
            """
            name: str = 'sample'
            db_configuration: DatabaseConnectionConfig = TestModel._db_configuration
            data_model_class: Type[DatabaseSourceDataModel] = TestDatabaseSourceDataModel

        source = DateBasedDatabaseSourceWithParams()
        response = source.get()

        self.assertIsInstance(response, Iterator)

        expected_data = [self.data[1], self.data[3]]
        expected_data.sort(key=lambda x: x['number'])

        received_data = list(response)
        received_data.sort(key=lambda x: x['number'])

        self.assertListEqual(expected_data, received_data)

    def tearDown(self) -> None:
        """Clean up after the tests"""
        try:
            delete_tables_from_database(db_configuration=TestModel._db_configuration,
                                        table_name=_TABLE_NAME, schema_name=_SCHEMA_NAME)
        except Exception:
            pass

        DatabaseConnection.close_all_connections()
        DatabaseConnection.remove_all_connections()


if __name__ == '__main__':
    main()
