import sqlite3
from typing import Type

import sqlalchemy as orm
from sqlalchemy.ext.declarative import declarative_base

from judah.destinations.database.config import DatabaseConnectionConfig
from judah.destinations.database.connection import DatabaseConnection


def create_tables_in_database(
        db_configuration: DatabaseConnectionConfig, model_base: Type[declarative_base], schema_name: str):
    """Creates the tables for the models in the database"""
    with DatabaseConnection.get_db_connection(
            db_connection_config=db_configuration) as db_connection:
        db_engine = db_connection.connection_engine
        try:
            db_engine.execute(orm.schema.CreateSchema(schema_name))
        except (sqlite3.OperationalError, orm.exc.OperationalError):
            db_engine.execute(f"ATTACH ':memory:' AS {schema_name}")
        except orm.exc.ProgrammingError:
            pass
        model_base.metadata.create_all(bind=db_engine)


def delete_tables_from_database(db_configuration: DatabaseConnectionConfig, table_name: str, schema_name: str):
    """Deletes the tables in the database"""
    with DatabaseConnection.get_db_connection(
            db_connection_config=db_configuration) as db_connection:
        db_engine = db_connection.connection_engine
        db_engine.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE')


def drop_schema(db_configuration: DatabaseConnectionConfig, schema_name: str):
    """Attempt to drop the schema"""
    with DatabaseConnection.get_db_connection(
            db_connection_config=db_configuration) as db_connection:
        db_engine = db_connection.connection_engine
        db_engine.execute(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')