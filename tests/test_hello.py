import unittest


class MyTestCase(unittest.TestCase):

    def test_add(self) -> None:
        assert 10 + 15 == 25
