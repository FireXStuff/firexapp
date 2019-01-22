
import unittest
from firexapp.common import delimit2list


class SplitListTests(unittest.TestCase):

    def test_delimit2list(self):
        self.assertEqual(delimit2list("1,2,3"), ["1", "2", "3"])
        self.assertEqual(delimit2list(None), [])
        self.assertEqual(delimit2list(""), [])
        self.assertEqual(delimit2list("one, 'two, two',three,,,',',',four,'"),
                         ['one', "two, two", 'three', 'four'])

        self.assertEqual(delimit2list('\'stuff\', convert_list_recursive1.txt', delimiters=","),
                         ['stuff', 'convert_list_recursive1.txt'])

        self.assertEqual(delimit2list("a_test_script.py --my_opt 'stuff1, stuff2', convert_list_recursive1.txt",
                                      delimiters=","),
                         ["a_test_script.py --my_opt 'stuff1, stuff2'", 'convert_list_recursive1.txt'])

        self.assertEqual(delimit2list('script.py --delimiter ","',
                                      delimiters=","),
                         ['script.py --delimiter ","'])

        self.assertEqual(delimit2list('a_test_script.py --my_opt "stuff1, stuff2", convert_list_recursive1.txt',
                                      delimiters=","),
                         ['a_test_script.py --my_opt "stuff1, stuff2"', 'convert_list_recursive1.txt'])

        self.assertEqual(delimit2list('a_test_script.py --my_opt "This is a sentence"', delimiters=","),
                         ['a_test_script.py --my_opt "This is a sentence"'])

        # test other delimiters
        self.assertEqual(delimit2list('one,two;three;four|five'),
                         ['one', 'two', 'three', 'four', 'five'])
        self.assertEqual(delimit2list('one\ntwo\nthree\nfour\nfive'),
                         ['one', 'two', 'three', 'four', 'five'])
