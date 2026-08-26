import unittest

from firex_workspace_smoke import workspace_identity


class WorkspaceSmokeTests(unittest.TestCase):
    def test_workspace_member_is_importable(self):
        self.assertEqual("firex-workspace-smoke", workspace_identity())


if __name__ == "__main__":
    unittest.main()
