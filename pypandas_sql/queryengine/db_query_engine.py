from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
from contextlib import closing

from pypandas_sql.dbconnector.db_connector import DBConnector

from pandas import DataFrame


class DBQueryEngine(ABC):

    def __init__(self, db_connector: DBConnector):
        self.db_connector = db_connector
        super(DBQueryEngine, self).__init__()

    @abstractmethod
    def get_pandas_df(self, sql: str, schema: Optional[str], parameters: Optional[List]) -> DataFrame:
        pass

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
