import os
import tempfile
import unittest
from firexapp.fileregistry import FileRegistry, KeyNotRegistered, KeyAlreadyRegistered
from firexapp.submit.uid import Uid


class FileRegistryTests(unittest.TestCase):

    def tearDown(self):
        FileRegistry().destroy()
        self.assertDictEqual(FileRegistry().file_registry, {})

    def test_registered_keys(self):
        key = 'key1'
        filename = 'something.txt'
        some_path = '/nobackup/user'
        FileRegistry().register_file(key, filename)

        with self.subTest('Basic functionality'):
            self.assertEqual(FileRegistry().get_file(key, some_path), os.path.join(some_path, filename))

        with self.subTest('Testing duplicate key registrations'):
            with self.assertRaises(KeyAlreadyRegistered):
                FileRegistry().register_file(key, 'something_else')
            self.assertEqual(FileRegistry().get_file(key, some_path), os.path.join(some_path, filename))

    def test_unregistered_key(self):
        some_path = '/nobackup/user'
        with self.assertRaises(KeyNotRegistered):
            FileRegistry().get_file('unregistered_key', some_path)

    def test_uid_object(self):
        uid = Uid()
        key = 'key3'
        relative_path = 'some_relative_path/something3.txt'
        FileRegistry().register_file(key, relative_path)

        try:
            with self.subTest('Basic functionality'):
                self.assertEqual(FileRegistry().get_file(key, uid), os.path.join(uid.logs_dir, relative_path))
        finally:
            try:
                os.removedirs(uid.logs_dir)
            except Exception:
                pass

    def test_dump_and_read_from_file(self):
        registry = {'key1': 'value1',
                    'key2': 'value2'}
        for k, v in registry.items():
            FileRegistry().register_file(k, v)

        file_registry = tempfile.NamedTemporaryFile().name
        FileRegistry().dump_to_file(file_registry)
        FileRegistry().destroy()
        FileRegistry(from_file=file_registry)
        self.assertDictEqual(FileRegistry().file_registry, registry)

