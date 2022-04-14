import unittest
from firexapp.submit.uid import is_firex_id


class UidTests(unittest.TestCase):

    def test_is_firex_id(self):
        self.assertTrue(is_firex_id('FireX-user-220413-233217-50289'))
        self.assertTrue(is_firex_id('FireX-user-220413-233217-50289999'))
        self.assertTrue(is_firex_id('FireX-user-220413-233217-5'))

        self.assertFalse(is_firex_id('FireX-user-name-220432-233217')) # 22/04/32 isn't a day
        self.assertFalse(is_firex_id('FireX-user-name-220413-233217'))
        self.assertFalse(is_firex_id('FireX-user-220413-233217'))
        self.assertFalse(is_firex_id('FireX-user-220413-233217-'))
