import os
import unittest
from firexapp.fileregistry import register_file, get_file, KeyNotRegistered, KeyAlreadyRegistered


class FileRegistryTests(unittest.TestCase):
    def test_default_callable(self):
        key = 'key1'
        filename = 'something.txt'
        some_path = '/nobackup/user'
        register_file(key, filename)

        with self.subTest('Basic functionality'):
            self.assertEqual(get_file(key, some_path), os.path.join(some_path, filename))

        with self.subTest('Testing duplicate key registrations'):
            with self.assertRaises(KeyAlreadyRegistered):
                register_file(key, 'something_else')
            self.assertEqual(get_file(key, some_path), os.path.join(some_path, filename))

    def test_unregistered_key(self):
        with self.assertRaises(KeyNotRegistered):
            get_file('unregistered_key')

    def test_non_default_callable(self):
        key = 'key2'
        filename = 'something2.txt'
        some_path = '/nobackup/user2'

        def foo(logs_dir):
            return os.path.join(logs_dir, filename)

        register_file(key, foo)
        self.assertEqual(get_file(key, some_path), foo(some_path))

    def test_non_default_callable_with_args(self):
        key1 = 'key3'
        key2 = 'key4'

        filename1 = 'something3.txt'
        filename2 = 'something4.txt'

        some_path = '/nobackup/user2'

        def bar(logs_dir, file=filename1):
            return os.path.join(logs_dir, file)

        with self.subTest('First registration of a partial'):
            register_file(key1, bar)
            self.assertEqual(get_file(key1, some_path), bar(some_path))

        with self.subTest('Seconds registration of a different partial of the same callable'):
            register_file(key2, bar, file=filename2)
            self.assertEqual(get_file(key2, some_path), bar(some_path, file=filename2))