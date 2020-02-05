import datetime
import os

from mock import patch, MagicMock

from pypandas_sql.queryengine.redshift_query_engine import RedshiftQueryEngine
from tests.redshift_model import Person
from tests.redshift_persistent_test_base import RedshiftPersistentTestBase


@patch('appdirs.user_config_dir', return_value=os.path.dirname(__file__))
@patch('keyring.get_password', return_value='random-password')
class RedshiftQueryEngineTest(RedshiftPersistentTestBase):

    def test_get_pandas_df(self, mock_keyring_get_password: MagicMock,
                           mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        with patch('pypandas_sql.dbconnector.redshift_connector.RedshiftConnector.get_connection',
                   return_value=self.get_connection()):
            query_engine = RedshiftQueryEngine()
            mock_user_config_dir.assert_called_once_with('pypandasql')
            mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
            df = query_engine.get_pandas_df(sql='select * from test_people', schema='test-schema')
            self.assertEqual(len(df), 1)
            self.assertEqual(df['user_name'].iloc[0], 'test')
            self.assertEqual(df['first_name'].iloc[0], 'first')
            self.assertEqual(df['last_name'].iloc[0], 'last')
            self.assertEqual(df['team'].iloc[0], 'test-team')
            self.assertEqual(df['employment_term'].iloc[0], 'fulltime')
            self.assertEqual(df['age'].iloc[0], 10)
            self.assertEqual(df['start_date'].iloc[0], '2020-02-02 00:00:00.000000')

    def test_get_pandas_df_empty_sql(self, mock_keyring_get_password: MagicMock,
                                     mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_pandas_df(sql='', schema='test-schema')

    def test_get_pandas_df_missing_sql(self, mock_keyring_get_password: MagicMock,
                                       mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_pandas_df(sql=None, schema='test-schema')

    def test_get_pandas_df_empty_schema(self, mock_keyring_get_password: MagicMock,
                                        mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_pandas_df(sql='select * from test_people', schema='')

    def test_get_pandas_df_missing_schema(self, mock_keyring_get_password: MagicMock,
                                          mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_pandas_df(sql='select * from test_people', schema=None)

    def test_get_records(self, mock_keyring_get_password: MagicMock,
                         mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        with patch('pypandas_sql.dbconnector.redshift_connector.RedshiftConnector.get_connection',
                   return_value=self.get_connection()):
            query_engine = RedshiftQueryEngine()
            mock_user_config_dir.assert_called_once_with('pypandasql')
            mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
            out_list = query_engine.get_records(sql='select * from test_people', schema='test-schema')
            self.assertEqual(len(out_list), 1)
            self.assertEqual(out_list[0][0], 'test')
            self.assertEqual(out_list[0][1], 'first')
            self.assertEqual(out_list[0][2], 'last')
            self.assertEqual(out_list[0][3], 'test-team')
            self.assertEqual(out_list[0][4], 'fulltime')
            self.assertEqual(out_list[0][5], '2020-02-02 00:00:00.000000')
            self.assertEqual(out_list[0][6], 10)

    def test_get_records_empty_sql(self, mock_keyring_get_password: MagicMock,
                                   mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_records(sql='', schema='test-schema')

    def test_get_records_df_missing_sql(self, mock_keyring_get_password: MagicMock,
                                        mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_records(sql=None, schema='test-schema')

    def test_get_records_df_empty_schema(self, mock_keyring_get_password: MagicMock,
                                         mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_records(sql='select * from test_people', schema='')

    def test_get_records_df_missing_schema(self, mock_keyring_get_password: MagicMock,
                                           mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_records(sql='select * from test_people', schema=None)

    def test_get_first_record(self, mock_keyring_get_password: MagicMock,
                              mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        with patch('pypandas_sql.dbconnector.redshift_connector.RedshiftConnector.get_connection',
                   return_value=self.get_connection()):
            query_engine = RedshiftQueryEngine()
            mock_user_config_dir.assert_called_once_with('pypandasql')
            mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
            out_list = query_engine.get_first_record(sql='select * from test_people', schema='test-schema')
            self.assertEqual(out_list[0], 'test')
            self.assertEqual(out_list[1], 'first')
            self.assertEqual(out_list[2], 'last')
            self.assertEqual(out_list[3], 'test-team')
            self.assertEqual(out_list[4], 'fulltime')
            self.assertEqual(out_list[5], '2020-02-02 00:00:00.000000')
            self.assertEqual(out_list[6], 10)

    def test_get_first_record_empty_sql(self, mock_keyring_get_password: MagicMock,
                                        mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_first_record(sql='', schema='test-schema')

    def test_get_first_record_df_missing_sql(self, mock_keyring_get_password: MagicMock,
                                             mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_first_record(sql=None, schema='test-schema')

    def test_get_first_record_df_empty_schema(self, mock_keyring_get_password: MagicMock,
                                              mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_first_record(sql='select * from test_people', schema='')

    def test_get_first_record_df_missing_schema(self, mock_keyring_get_password: MagicMock,
                                                mock_user_config_dir: MagicMock):
        persons = [Person(user_name='test', first_name='first', last_name='last', team='test-team',
                          employment_term='fulltime', age=10, start_date=datetime.datetime(2020, 2, 2))]
        self.add_data(persons)
        query_engine = RedshiftQueryEngine()
        mock_user_config_dir.assert_called_once_with('pypandasql')
        mock_keyring_get_password.assert_called_once_with('pypandasql-redshift', 'test')
        with self.assertRaises(AssertionError):
            query_engine.get_first_record(sql='select * from test_people', schema=None)
