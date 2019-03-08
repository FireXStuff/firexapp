import os
import unittest
# noinspection PyProtectedMember
from tempfile import NamedTemporaryFile, _get_candidate_names, gettempdir
from threading import Timer

from firexapp.common import delimit2list, poll_until_file_exist, poll_until_file_not_empty


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


class PollingTests(unittest.TestCase):

    @staticmethod
    def touch_file(file_path, content=None):
        with open(file_path, "a+") as f:
            if content:
                f.write(content)

    def test_poll_till_exists(self):
        # Never going to arrive
        with self.assertRaises(AssertionError):
            poll_until_file_exist(os.path.join(os.path.dirname(__file__), "not_happening"), timeout=0.1)

        # already exists
        poll_until_file_exist(__file__, timeout=0.1)

        file_name = os.path.join(gettempdir(), next(_get_candidate_names()))
        try:
            t = Timer(0.2, lambda: self.touch_file(file_name))
            t.start()
            assert not os.path.isfile(file_name), "File should not exist before the poll starts"
            poll_until_file_exist(file_name, timeout=0.5)
            self.assertTrue(os.path.exists(file_name))
            t.join(timeout=1)
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)

    def test_poll_till_content(self):
        # Never going to arrive
        # todo: are we to handle the case here the file does not already exist?
        # with self.assertRaises(AssertionError):
        #     poll_until_file_not_empty(os.path.join(os.path.dirname(__file__), "not_happening"), timeout=0.1)

        # Never going to have content
        with self.assertRaises(AssertionError):
            with NamedTemporaryFile(delete=True) as f:
                poll_until_file_not_empty(f.name, timeout=0.1)

        # already exists and has content
        poll_until_file_exist(__file__, timeout=0.1)

        with NamedTemporaryFile(delete=True) as f:
            file_name = f.name
            t = Timer(0.2, lambda: f.write(b"hello world") and f.flush())
            t.start()
            poll_until_file_not_empty(file_name, timeout=0.5)
            t.join(timeout=1)



