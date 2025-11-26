import unittest
from celery import Celery
from celery.exceptions import NotRegistered
from firexkit.task import FireXTask
from firexapp.engine.firex_celery import FireXCelery

class GetTasksTests(unittest.TestCase):

    def test_get_app_task(self):
        test_app = Celery(set_as_current=False)

        all_tasks = test_app.tasks

        # look up something that isn't there
        with self.assertRaises(NotRegistered):
            FireXCelery.get_app_task("task_a", all_tasks)

        @test_app.task(base=FireXTask)
        def task_a():
            pass  # pragma: no cover

        # look up something that is
        self.assertIsNotNone(FireXCelery.get_app_task("task_a", all_tasks))

        # look up with extra padding
        self.assertIsNotNone(FireXCelery.get_app_task("      task_a     ", all_tasks))

        # look up without all_tasks
        from firexapp.engine.celery import app
        self.assertIsNotNone(app.get_task("group"))

        # lookup case incentive
        self.assertIsNotNone(FireXCelery.get_app_task("TASK_A", all_tasks))

        @test_app.task(base=FireXTask)
        def task_b():
            pass  # pragma: no cover

        # lookup multiple
        found_tasks = FireXCelery.get_app_tasks("task_a,task_b", all_tasks)
        self.assertEqual(len(found_tasks), 2)
        found_tasks = FireXCelery.get_app_tasks(["task_a", "task_b"], all_tasks)
        self.assertEqual(len(found_tasks), 2)
