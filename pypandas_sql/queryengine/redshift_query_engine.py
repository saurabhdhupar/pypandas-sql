from typing import List, Tuple, Optional
from contextlib import closing

from pandas import DataFrame
import pandas.io.sql as psql

from pypandas_sql.queryengine.db_query_engine import DBQueryEngine
from pypandas_sql.dbconnector.redshift_connector import RedshiftConnector


class RedshiftQueryEngine(DBQueryEngine):

    def __init__(self) -> None:
        super(RedshiftQueryEngine, self).__init__(db_connector=RedshiftConnector())

    def get_pandas_df(self, sql: str, schema: Optional[str], parameters: Optional[List] = None) -> DataFrame:
        assert sql is not None and len(sql) > 0
        conn = self.db_connector.get_connection(schema)
        with closing(conn):
            return psql.read_sql(sql, con=conn, params=parameters)

    def get_records(self, sql: str, schema: Optional[str], parameters: Optional[List] = None) -> List[Tuple]:
        assert sql is not None and len(sql) > 0
        conn = self.db_connector.get_connection(schema)
        with closing(conn):
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchall()

    def get_first_record(self, sql: str, schema: Optional[str], parameters: Optional[List] = None) -> List[Tuple]:
        assert sql is not None and len(sql) > 0
        conn = self.db_connector.get_connection(schema)
        with closing(conn):
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchone()
