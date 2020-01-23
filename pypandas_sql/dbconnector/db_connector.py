from abc import ABC, abstractmethod


class DBConnector(ABC):

    def __init__(self, engine_name: str, connection_attr: dict):
        assert engine_name is not None and len(engine_name) != 0
        assert connection_attr is not None and bool(connection_attr)
        self.connection_attr = connection_attr
        self.engine_name = engine_name
        super(DBConnector, self).__init__()

    @abstractmethod
    def get_uri(self):
        pass

    @abstractmethod
    def get_engine(self, engine_kwargs=None):
        pass

    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    def get_cursor(self):
        pass
