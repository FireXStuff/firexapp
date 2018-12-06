import os
import unittest
from firexapp.discovery import discover_package_modules


class DiscoveryTests(unittest.TestCase):
    def test_discover_package_modules(self):
        fake_package_location = os.path.join(os.path.dirname(__file__), "data", "discovery")

        modules_found = discover_package_modules(fake_package_location)
        self.assertIsNotNone(modules_found)
        self.assertEqual(len(modules_found), 2)
        self.assertTrue("discovery.top_module" in modules_found)
        self.assertTrue("discovery.sub_module1.leaf_module" in modules_found)
