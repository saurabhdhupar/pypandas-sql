from typing import Any

from tests.persistent_test_base import PersistentTestBase
from tests.redshift_model import Base


class RedshiftPersistentTestBase(PersistentTestBase):

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(Base, *args, **kwargs)
