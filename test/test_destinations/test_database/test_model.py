"""
Module containing tests for the DatabaseBaseModel class in the model module
in the database destination package
"""
import os
from datetime import date
from typing import Dict, Type, Optional, List
from unittest import TestCase, main

import sqlalchemy as orm
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

from judah.destinations.database.connection import DatabaseConnection
from judah.destinations.database.config import DatabaseConnectionConfig
from judah.destinations.database.model import DatabaseBaseModel
from test.utils import create_tables_in_database, delete_tables_from_database, drop_schema

_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

load_dotenv(os.path.join(_ROOT_PATH, '.env'))

_TEST_DB_URI = os.environ.get('TEST_POSTGRES_DB_URI')

_TEST_DB_BASE = declarative_base()
_TABLE_NAME = 'dummy'
_SCHEMA_NAME = 'test_schema'


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


class TestDatabaseBaseModel(TestCase):
    """Tests for the DatabaseBaseModel base class"""

    def setUp(self) -> None:
        """Initialize a few variables"""
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

    def test_get_attributes(self):
        """Should return the column names of the model"""
        self.load_database()

        column_names = TestModel.get_attributes()
        expected_columns = ['Date', 'number', 'Capacity', 'Price', 'created_at', 'updated_at']
        self.assertListEqual(sorted(column_names), sorted(expected_columns))

    def test_get_last_record(self):
        """Should return the latest record according to the given datetime column"""
        self.load_database()

        last_record = TestModel.get_last_record()
        expected_last_record = self.data[1]

        columns = ['Date', 'number', 'Capacity', 'Price']
        for column in columns:
            self.assertEqual(getattr(last_record, column), expected_last_record[column])

    def test_get_last_saved_timestamp(self):
        """Should return the timestamp of the last saved record"""
        self.load_database()

        last_timestamp = TestModel.get_last_saved_timestamp()
        expected_last_timestamp = self.data[1]['Date']

        self.assertEqual(last_timestamp, expected_last_timestamp)

    def test_update(self):
        """Should update the attributes passed in the kwargs and saves"""
        create_tables_in_database(db_configuration=TestModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            record = TestModel(**self.data[1])
            db_connection.db_session.add(record)
            db_connection.db_session.commit()

            new_capacity = 56
            new_price = 7

            record.update(session=db_connection.db_session, Capacity=new_capacity, Price=new_price)

            record_from_database = db_connection.db_session.query(TestModel).filter_by(
                Date=record.Date, number=record.number).first()
            db_connection.db_session.commit()

            self.assertEqual(record_from_database.Capacity, new_capacity)
            self.assertEqual(record_from_database.Price, new_price)

    def test_save(self):
        """Should commit any new changes to the database"""
        create_tables_in_database(db_configuration=TestModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            record = TestModel(**self.data[1])

            record_from_database_pre_save = db_connection.db_session.query(TestModel).filter_by(
                Date=record.Date, number=record.number).first()
            db_connection.db_session.commit()

            record.save(db_connection.db_session)

            record_from_database_post_save = db_connection.db_session.query(TestModel).filter_by(
                Date=record.Date, number=record.number).first()
            db_connection.db_session.commit()

            self.assertIsNone(record_from_database_pre_save)
            self.assertIsInstance(record_from_database_post_save, TestModel)

    def test_delete(self):
        """Should delete the current record from the database"""
        create_tables_in_database(db_configuration=TestModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            record = TestModel(**self.data[1])
            db_connection.db_session.add(record)
            db_connection.db_session.commit()

            record_from_database_pre_deletion = db_connection.db_session.query(TestModel).filter_by(
                Date=record.Date, number=record.number).first()

            record.delete(db_connection.db_session)

            record_from_database_post_deletion = db_connection.db_session.query(TestModel).filter_by(
                Date=record.Date, number=record.number).first()

            self.assertIsNone(record_from_database_post_deletion)
            self.assertIsInstance(record_from_database_pre_deletion, TestModel)

    def test_create_schema(self):
        """Should create the schema for this class if it exists"""
        drop_schema(db_configuration=TestModel._db_configuration, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            schema_check_sql = f"""
            SELECT exists(select schema_name FROM information_schema.schemata WHERE schema_name = '{_SCHEMA_NAME}')
            """
            self.assertFalse(db_connection.execute_sql(schema_check_sql).first()[0])

            TestModel.create_schema(db_connection.connection_engine)

            self.assertTrue(db_connection.execute_sql(schema_check_sql).first()[0])

    def test_initialize(self):
        """Should create the tables in the database"""
        drop_schema(db_configuration=TestModel._db_configuration, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            table_check_sql = f"""
            SELECT EXISTS (
               SELECT FROM pg_tables
               WHERE  schemaname = '{_SCHEMA_NAME}'
               AND    tablename  = '{_TABLE_NAME}'
               )
            """
            self.assertFalse(db_connection.execute_sql(table_check_sql).first()[0])

            TestModel.initialize()

            self.assertTrue(db_connection.execute_sql(table_check_sql).first()[0])

    def test_upsert_new_record(self):
        """upsert should creates a new record if it does not exist and then return the data"""
        create_tables_in_database(db_configuration=TestModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            raw_data = self.data[1]
            record_from_database_pre_insert = db_connection.db_session.query(TestModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            db_connection.db_session.commit()
            self.assertIsNone(record_from_database_pre_insert)

            recorded_data = TestModel.upsert(raw_data)
            record_from_database_post_insert = db_connection.db_session.query(TestModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            db_connection.db_session.commit()
            self.assertDictEqual(recorded_data, raw_data)
            self.assertIsInstance(record_from_database_post_insert, TestModel)

            for field, value in raw_data.items():
                self.assertEqual(getattr(record_from_database_post_insert, field), value)

    def test_upsert_old_record(self):
        """upsert should update an existing record if it does not exist and then return the data"""
        create_tables_in_database(db_configuration=TestModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=TestModel._db_configuration) as db_connection:
            raw_data = self.data[1]
            record = TestModel(**raw_data)
            db_connection.db_session.add(record)
            db_connection.db_session.commit()

            new_data = {
                **raw_data,
                'Capacity': 7643,
                'Price': 211
            }
            updated_data = TestModel.upsert(new_data)
            record_from_database_post_update = db_connection.db_session.query(TestModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            self.assertDictEqual(updated_data, new_data)
            self.assertIsInstance(record_from_database_post_update, TestModel)

            for field, value in new_data.items():
                self.assertEqual(getattr(record_from_database_post_update, field), value)

    def tearDown(self) -> None:
        try:
            delete_tables_from_database(db_configuration=TestModel._db_configuration,
                                        table_name=_TABLE_NAME, schema_name=_SCHEMA_NAME)
        except Exception:
            pass

        DatabaseConnection.close_all_connections()
        DatabaseConnection.remove_all_connections()


if __name__ == '__main__':
    main()
