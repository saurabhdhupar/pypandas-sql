import unittest

from mock import patch

from pypandas_sql.dbconnector.db_connector import DBConnector
from pypandas_sql.queryengine.db_query_engine import DBQueryEngine


class DBQueryEngineTest(unittest.TestCase):

    @patch.multiple(DBQueryEngine, __abstractmethods__=set())
    @patch.multiple(DBConnector, __abstractmethods__=set())
    def test_db_query_engine_creation(self):
        connection = {'host': 'test-host', 'port': 123, 'user': 'test-user'}
        db_connector = DBConnector(engine_name='test_engine', connection_attr=connection)
        query_engine = DBQueryEngine(db_connector=db_connector)
        self.assertIsNotNone(query_engine)
        self.assertEqual(query_engine.db_connector, db_connector)
