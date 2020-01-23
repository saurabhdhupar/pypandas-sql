from typing import List, Tuple, Optional
from contextlib import closing

import pandas.io.sql as psql

from pypandas_sql.queryengine.db_query_engine import DBQueryEngine
from pypandas_sql.dbconnector.redshift_connector import RedshiftConnector


class RedshiftQueryEngine(DBQueryEngine):

    def __init__(self):
        db_connector = RedshiftConnector()
        super(self).__init__(db_connector=db_connector)

    def get_pandas_df(self, sql: str, parameters: Optional[List]) -> DataFrame:
        conn = self.db_connector.get_connection()
        with closing(conn):
            return psql.read_sql(sql, con=conn, params=parameters)

    def get_records(self, sql: str, parameters: Optional[List]) -> List[Tuple]:
        conn = self.db_connector.get_connection()
        with closing(conn):
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchall()

    def get_first_record(self, sql: str, parameters: Optional[List]) -> List[Tuple]:
        conn = self.db_connector.get_connection()
        with closing(conn):
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchone()
