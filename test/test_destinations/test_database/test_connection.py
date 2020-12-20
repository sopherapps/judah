"""
Module containing tests for the DatabaseConnection class in the connection module
in the database destination package
"""
import os
from unittest import TestCase, main
from unittest.mock import Mock

from sqlalchemy.orm import scoped_session

from judah.destinations.database.connection import DatabaseConnection
from judah.destinations.database.config import DatabaseConnectionConfig

_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(os.path.dirname(_PARENT_FOLDER)), 'assets')


class TestDatabaseConnection(TestCase):
    """Tests for the DatabaseConnection base class"""

    def setUp(self) -> None:
        """Initialize a few variables"""
        self.test_db_1_uri = f'sqlite:///{_MOCK_ASSET_FOLDER_PATH}/test_db_1'
        self.test_db_2_uri = f'sqlite:///{_MOCK_ASSET_FOLDER_PATH}/test_db_2'
        self.test_db_1_config = DatabaseConnectionConfig(db_uri=self.test_db_1_uri)
        self.test_db_2_config = DatabaseConnectionConfig(db_uri=self.test_db_2_uri)

    def test_context_manager_enter(self):
        """On entering the context manager, a scoped session is created and saved to the db_session attribute"""
        test_db_1_connection = DatabaseConnection(self.test_db_1_config)
        self.assertIsNone(test_db_1_connection.db_session)

        with DatabaseConnection(self.test_db_2_config) as test_db_2_connection:
            self.assertIsInstance(test_db_2_connection.db_session, scoped_session)

    def test_context_manager_exit(self):
        """On exiting the context manager, the db_session is closed"""
        with DatabaseConnection(self.test_db_1_config) as test_db_1_connection:
            # create a dummy session to add to the scoped_session's registry
            dummy_session = test_db_1_connection.db_session()
            self.assertTrue(test_db_1_connection.db_session.registry.has())

        self.assertFalse(test_db_1_connection.db_session.registry.has())

    def test_context_manager_exit_with_no_autocommit(self):
        """
        On exiting the context manager when self.db_connection_config.autocommit is False,
        the changes are committed before the session is closed when exiting the context manager.
        """
        db_config_with_no_autocommit = DatabaseConnectionConfig(db_uri=self.test_db_1_uri, autocommit=False)
        with DatabaseConnection(db_config_with_no_autocommit) as db_connection:
            db_connection.db_session.commit = Mock()

        db_connection.db_session.commit.assert_called_once()

    def test_context_manager_exit_with_autocommit(self):
        """
        On exiting the context manager when self.db_connection_config.autocommit is True,
        the changes are not committed before the session is closed when exiting the context manager.
        """
        db_config_with_autocommit = DatabaseConnectionConfig(db_uri=self.test_db_1_uri, autocommit=True)
        with DatabaseConnection(db_config_with_autocommit) as db_connection:
            db_connection.db_session.commit = Mock()

        db_connection.db_session.commit.assert_not_called()

    def test_execute_sql(self):
        """Should execute the sql passed as a parameter"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dummy ( id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT)
        """
        delete_sql = """
        DELETE FROM dummy WHERE name = :name
        """
        insert_sql = """
        INSERT INTO dummy (name) VALUES (:name)
        """
        select_sql = """
        SELECT * FROM dummy
        """
        test_name = 'John Doe'

        with DatabaseConnection(self.test_db_1_config) as db_connection:
            db_connection.execute_sql(create_table_sql)
            db_connection.execute_sql(insert_sql, {'name': test_name})
            names = [item.name for item in db_connection.execute_sql(select_sql).fetchmany()]
            self.assertEqual(names[0], test_name)
            db_connection.execute_sql(delete_sql, {'name': test_name})

    def test_get_db_connection(self):
        """Should get the already initialized instance of DatabaseConnection with the given db_uri"""
        test_db_connection = DatabaseConnection(self.test_db_1_config)
        self.assertEqual(test_db_connection, DatabaseConnection.get_db_connection(self.test_db_1_config))

    def test_close(self):
        """Closes the db connection engine"""
        db_connection = DatabaseConnection(self.test_db_1_config)
        old_dispose = db_connection.connection_engine.dispose
        db_connection.connection_engine.dispose = Mock()
        db_connection.connection_engine.dispose.side_effect = old_dispose

        db_connection.close()
        db_connection.connection_engine.dispose.assert_called_once()

    def test_close_all_connections(self):
        """Should close all connections in the dictionary of connections"""
        db_connection_1 = DatabaseConnection(self.test_db_1_config)
        db_connection_2 = DatabaseConnection(self.test_db_2_config)

        old_dispose_1 = db_connection_1.connection_engine.dispose
        db_connection_1.connection_engine.dispose = Mock()
        db_connection_1.connection_engine.dispose.side_effect = old_dispose_1

        old_dispose_2 = db_connection_2.connection_engine.dispose
        db_connection_2.connection_engine.dispose = Mock()
        db_connection_2.connection_engine.dispose.side_effect = old_dispose_2

        DatabaseConnection.close_all_connections()
        db_connection_1.connection_engine.dispose.assert_called_once()
        db_connection_2.connection_engine.dispose.assert_called_once()

    def test_open_connections(self):
        """Should open the database connections whose configs have been passed to it"""
        self.assertDictEqual(DatabaseConnection.connections, {})
        DatabaseConnection.open_connections([self.test_db_1_config, self.test_db_2_config])

        self.assertEqual(len(DatabaseConnection.connections), 2)
        self.assertIsInstance(DatabaseConnection.connections[self.test_db_1_uri], DatabaseConnection)
        self.assertIsInstance(DatabaseConnection.connections[self.test_db_2_uri], DatabaseConnection)

    def test_remove_connection(self):
        """Should remove a database connection of a given db_uri from the dictionary of connections"""
        db_connection_1 = DatabaseConnection(self.test_db_1_config)
        db_connection_2 = DatabaseConnection(self.test_db_2_config)

        self.assertIsInstance(DatabaseConnection.connections[self.test_db_1_uri], DatabaseConnection)

        DatabaseConnection.remove_connection(self.test_db_1_uri)

        self.assertIsNone(DatabaseConnection.connections.get(self.test_db_1_uri, None))
        self.assertIsInstance(DatabaseConnection.connections[self.test_db_2_uri], DatabaseConnection)

    def test_remove_all_connections(self):
        """Should remove all database connections in the dictionary of connections"""
        db_connection_1 = DatabaseConnection(self.test_db_1_config)
        db_connection_2 = DatabaseConnection(self.test_db_2_config)

        self.assertIsInstance(DatabaseConnection.connections[self.test_db_1_uri], DatabaseConnection)

        DatabaseConnection.remove_all_connections()

        self.assertIsNone(DatabaseConnection.connections.get(self.test_db_1_uri, None))
        self.assertIsNone(DatabaseConnection.connections.get(self.test_db_2_uri, None))

    def tearDown(self) -> None:
        DatabaseConnection.close_all_connections()
        DatabaseConnection.remove_all_connections()


if __name__ == '__main__':
    main()
