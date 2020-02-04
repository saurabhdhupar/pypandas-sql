import os
import unittest

from mock import patch, MagicMock

from pypandas_sql.dbconnector.redshift_connector import RedshiftConnector


@patch('appdirs.user_config_dir', return_value=os.path.dirname(__file__))
@patch('keyring.get_password', return_value='random-password')
class RedshiftConnectorTest(unittest.TestCase):

    def test_redshift_connector_creation(self, mock_keyring_get_password: MagicMock,
                                         mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        self.assertEqual(redshift_connector.engine_name, 'redshift+psycopg2')
        connection_att = redshift_connector.connection_attr
        self.assertEqual(connection_att['host'], 'test-host')
        self.assertEqual(connection_att['port'], '1234')
        self.assertEqual(connection_att['user_name'], 'test')
        credentials = connection_att['credentials']
        self.assertEqual(credentials.service_name, 'pypandasql-redshift')
        self.assertEqual(credentials.user, 'test')
        self.assertEqual(credentials.password, 'random-password')

    def test_get_uri(self, mock_keyring_get_password: MagicMock,
                     mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        self.assertEqual(redshift_connector.get_uri(schema='test-schema'),
                         'redshift+psycopg2://test:random-password@test-host:1234/test-schema')

    def test_get_uri_with_empty_schema(self, mock_keyring_get_password: MagicMock,
                                       mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_uri(schema='')

    def test_get_uri_with_missing_schema(self, mock_keyring_get_password: MagicMock,
                                         mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_uri(schema=None)

    @patch('sqlalchemy.create_engine')
    def test_get_engine(self, mock_create_engine: MagicMock,
                        mock_keyring_get_password: MagicMock,
                        mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        redshift_connector.get_engine(schema='test-schema')
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        mock_create_engine.assert_called_once_with('redshift+psycopg2://test:random-password@'
                                                   'test-host:1234/test-schema')

    def test_get_engine_missing_schema(self, mock_keyring_get_password: MagicMock,
                                       mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_engine(schema=None)

    def test_get_engine_empty_schema(self, mock_keyring_get_password: MagicMock,
                                     mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_engine(schema='')

    @patch('psycopg2.connect')
    def test_get_connection(self, mock_connect: MagicMock,
                            mock_keyring_get_password: MagicMock,
                            mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        redshift_connector.get_connection(schema='test-schema')
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        mock_connect.assert_called_once_with(dbname='test-schema',
                                             host='test-host',
                                             port=1234,
                                             user='test',
                                             password='random-password')

    def test_get_connection_missing_schema(self, mock_keyring_get_password: MagicMock,
                                           mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_connection(schema=None)

    def test_get_connection_empty_schema(self, mock_keyring_get_password: MagicMock,
                                         mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_connection(schema='')

    @patch('psycopg2.connect')
    def test_get_cursor(self, mock_connect: MagicMock,
                        mock_keyring_get_password: MagicMock,
                        mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        redshift_connector.get_cursor(schema='test-schema')
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        mock_connect.assert_called_once_with(dbname='test-schema',
                                             host='test-host',
                                             port=1234,
                                             user='test',
                                             password='random-password')

    def test_get_cursor_missing_schema(self, mock_keyring_get_password: MagicMock,
                                       mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_cursor(schema=None)

    def test_get_cursor_empty_schema(self, mock_keyring_get_password: MagicMock,
                                     mock_user_config_dir: MagicMock) -> None:
        redshift_connector = RedshiftConnector()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            redshift_connector.get_cursor(schema='')
