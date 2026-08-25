import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from firexapp.engine.autoscaler import FireXAutoscaler


class FireXAutoscalerTests(unittest.TestCase):
    @staticmethod
    def create_autoscaler(reserved=(), active=(), revoked=()):
        worker = SimpleNamespace(app=Mock(), state=SimpleNamespace(
            reserved_requests=[SimpleNamespace(id=task_id) for task_id in reserved],
            active_requests=[SimpleNamespace(id=task_id) for task_id in active],
            revoked=set(revoked),
        ))
        return FireXAutoscaler(Mock(), max_concurrency=4, worker=worker, check_freq=1)

    @patch.object(FireXAutoscaler, '_get_task_postrun_info', return_value=None)
    @patch.object(FireXAutoscaler, '_get_task_prerun_info', return_value={'started': True})
    def test_counts_revoked_task_until_postrun(self, _prerun, _postrun):
        autoscaler = self.create_autoscaler(revoked=('running-revoked',))

        self.assertEqual(1, autoscaler.qty)

    @patch.object(FireXAutoscaler, '_get_task_postrun_info', return_value={'done': True})
    @patch.object(FireXAutoscaler, '_get_task_prerun_info', return_value={'started': True})
    def test_stops_counting_revoked_task_after_postrun(self, _prerun, _postrun):
        autoscaler = self.create_autoscaler(revoked=('completed-revoked',))

        self.assertEqual(0, autoscaler.qty)

    @patch.object(FireXAutoscaler, '_get_task_postrun_info')
    @patch.object(FireXAutoscaler, '_get_task_prerun_info', return_value=None)
    def test_does_not_count_revoked_task_that_never_started(self, _prerun, postrun):
        autoscaler = self.create_autoscaler(revoked=('unstarted-revoked',))

        self.assertEqual(0, autoscaler.qty)
        postrun.assert_not_called()