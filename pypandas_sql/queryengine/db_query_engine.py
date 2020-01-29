from abc import ABC, abstractmethod
from typing import List, Tuple, Optional

from pypandas_sql.dbconnector.db_connector import DBConnector

from pandas import DataFrame


class DBQueryEngine(ABC):

    def __init__(self, db_connector: DBConnector):
        self.db_connector = db_connector
        super(DBQueryEngine, self).__init__()

    @abstractmethod
    def get_pandas_df(self, sql: str, schema: Optional[str], parameters: Optional[List]) -> DataFrame:
        pass

    @abstractmethod
    def get_records(self, sql: str, schema: Optional[str], parameters: Optional[List]) -> List[Tuple]:
        pass

    @abstractmethod
    def get_first_record(self, sql: str, schema: Optional[str], parameters: Optional[List]) -> List[Tuple]:
        pass
