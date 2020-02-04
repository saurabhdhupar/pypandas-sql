import unittest

from mock import patch

from pypandas_sql.dbconnector.db_connector import DBConnector


class DBConnectorTest(unittest.TestCase):

    @patch.multiple(DBConnector, __abstractmethods__=set())
    def test_db_connector_object_creation(self) -> None:
        connection = {'host': 'test-host', 'port': 123, 'user': 'test-user'}
        db_connector = DBConnector(engine_name='test_engine', connection_attr=connection)
        self.assertIsNotNone(db_connector)
        self.assertEqual(db_connector.engine_name, 'test_engine')
        self.assertEqual(db_connector.connection_attr, connection)

    @patch.multiple(DBConnector, __abstractmethods__=set())
    def test_db_connector_empty_engine_name(self) -> None:
        connection = {'host': 'test-host', 'port': 123, 'user': 'test-user'}
        with self.assertRaises(AssertionError):
            DBConnector(engine_name='', connection_attr=connection)

    @patch.multiple(DBConnector, __abstractmethods__=set())
    def test_db_connector_missing_engine_name(self) -> None:
        connection = {'host': 'test-host', 'port': 123, 'user': 'test-user'}
        with self.assertRaises(AssertionError):
            DBConnector(engine_name=None, connection_attr=connection)

    @patch.multiple(DBConnector, __abstractmethods__=set())
    def test_db_connector_empty_connection_attr(self) -> None:
        with self.assertRaises(AssertionError):
            DBConnector(engine_name='test_engine', connection_attr={})

    @patch.multiple(DBConnector, __abstractmethods__=set())
    def test_db_connector_missing_connection_attr(self) -> None:
        with self.assertRaises(AssertionError):
            DBConnector(engine_name='test_engine', connection_attr=None)
