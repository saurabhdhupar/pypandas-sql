from abc import ABC, abstractmethod
from typing import Any, Optional

import sqlalchemy


class DBConnector(ABC):

    def __init__(self, engine_name: str, connection_attr: dict):
        assert engine_name is not None and len(engine_name) > 0
        assert connection_attr is not None and bool(connection_attr)
        self.connection_attr = connection_attr
        self.engine_name = engine_name
        super(DBConnector, self).__init__()

    @abstractmethod
    def get_uri(self, schema: Optional[str]) -> str:
        pass

    def get_engine(self, schema: Optional[str]) -> Any:
        assert schema is not None and len(schema) > 0
        return sqlalchemy.create_engine(self.get_uri(schema))

    @abstractmethod
    def get_connection(self, schema: Optional[str]) -> Any:
        pass

    def get_cursor(self, schema: Optional[str]) -> Any:
        assert schema is not None and len(schema) > 0
        return self.get_connection(schema).cursor()
