import unittest
from collections import Collection
from typing import Any

import sqlalchemy


class PersistentTestBase(unittest.TestCase):

    def __init__(self, base: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:')
        base.metadata.create_all(self.engine)
        self.sessionmaker_ = sqlalchemy.orm.sessionmaker(bind=self.engine)

    def add_data(self, entries: Collection):
        session_ = self.sessionmaker_()
        try:
            session_.add_all(entries)
            session_.commit()
        except BaseException as e:
            session_.rollback()
            raise e
        finally:
            session_.close()

    def get_connection(self) -> Any:
        return self.engine.raw_connection()
