import unittest

from mymodule import add


class TestAdd(unittest.TestCase):
    def test_add(self):
        self.assertEquals(add(9,8), 17)
        self.assertEquals(add(1,8), 9)
        self.assertEquals(add(0,0), 0)

unittest.main()