"""
Database Connector class that provides a context manager for database connections
"""
import logging
from typing import Dict, Any, List, Optional

from sqlalchemy import create_engine, engine
from sqlalchemy.orm import scoped_session, sessionmaker, Session
from sqlalchemy.sql import text

from .config import DatabaseConnectionConfig


class DatabaseConnection:
    """
    Connects to a given database, returning a session in a context manager, and runs a given query
    """
    connections = {}

    def __init__(self, db_connection_config: DatabaseConnectionConfig):
        self.db_uri = db_connection_config.db_uri
        self.db_connection_config = db_connection_config
        self.db_session: Optional[scoped_session] = None

        if db_connection_config.db_uri.startswith('sqlite'):
            self.connection_engine: engine.Engine = create_engine(self.db_uri)
        else:
            self.connection_engine: engine.Engine = create_engine(
                self.db_uri,
                pool_size=self.db_connection_config.pool_size,
                max_overflow=self.db_connection_config.pool_size + 5,
                pool_timeout=self.db_connection_config.pool_timeout,
            )
        DatabaseConnection.connections[self.db_uri] = self

    def __enter__(self):
        """
        Creates a scoped session for the database that is thread-safe for reads
        """
        session = sessionmaker(
            bind=self.connection_engine,
            autocommit=self.db_connection_config.autocommit,
            autoflush=self.db_connection_config.autocommit,
            expire_on_commit=False,
        )
        self.db_session = scoped_session(session)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This runs at the end of the 'with' statement as a way of cleaning up
        """
        if not self.db_connection_config.autocommit:
            # Attempt to commit any changes
            self.db_session.commit()

        self.db_session.remove()

    def close(self):
        """
        Closes the db connection engine
        """
        try:
            self.connection_engine.dispose()
            logging.info(f'Successfully closed connection {self.db_uri}')
        except Exception as exp:
            logging.warning(str(exp))

    def execute_sql(self, sql: str, params: Dict[str, Any] = {}) -> engine.ResultProxy:
        """
        Executes the sql passed as a parameter
        """
        return self.db_session.execute(text(sql), params=params)

    @classmethod
    def get_db_connection(cls, db_connection_config: DatabaseConnectionConfig):
        """
        Returns an instance of this class that has the given db_uri or if none, it creates a new one
        """
        db_connection = cls.connections.get(db_connection_config.db_uri, None)

        if db_connection is None:
            db_connection = cls(db_connection_config=db_connection_config)

        return db_connection

    @classmethod
    def close_all_connections(cls):
        """
        Closes all connections
        """
        for _, db_connection in cls.connections.items():
            db_connection.close()

    @classmethod
    def remove_connection(cls, db_uri: str):
        """
        Removes the given connection from the dictionary of connections
        """
        cls.connections.pop(db_uri, None)

    @classmethod
    def remove_all_connections(cls):
        """
        Removes all connection from the dictionary of connections
        """
        cls.connections.clear()

    @classmethod
    def open_connections(cls, db_configs: List[DatabaseConnectionConfig]):
        """
        Opens database connections if they are not open
        """
        for db_config in db_configs:
            cls.get_db_connection(db_connection_config=db_config)
