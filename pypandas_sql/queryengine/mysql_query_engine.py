from contextlib import closing
from typing import Optional, List

from pandas import DataFrame

from pypandas_sql.dbconnector.mysql_connector import MySQLConnector
from pypandas_sql.queryengine.db_query_engine import DBQueryEngine


class MySQLQueryEngine(DBQueryEngine):

    def __init__(self) -> None:
        super(MySQLQueryEngine, self).__init__(db_connector=MySQLConnector())

    def get_pandas_df(self, sql: str, schema: Optional[str], parameters: Optional[List] = None) -> DataFrame:
        cursor = self.db_connector.get_cursor(schema)
        if parameters:
            cursor.execute(sql, args=parameters)
        else:
            cursor.execute(sql)
        columns_list = [desc[0] for desc in cursor.description]
        with closing(cursor):
            return DataFrame(cursor.fetchall(), columns=columns_list)
