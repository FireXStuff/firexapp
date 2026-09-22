import contextlib
import json
import os
import tempfile
import unittest
from unittest import mock

from firexapp.reporters.json_reporter import (
    FireXRunData,
    _get_initial_run_json_path,
    _write_run_json,
)


def _run_data(logs_dir, **overrides) -> FireXRunData:
    return FireXRunData(
        firex_id="FireX-someuser-260914-120000-1234",
        logs_path=logs_dir,
        completed=False,
        chain=["nop"],
        submission_host="somehost",
        submission_dir="/some/dir",
        submission_cmd=["firexapp", "submit", "--chain", "nop"],
        viewers={},
        inputs={"soft_time_limit": 40 * 60 * 60},
        **overrides,
    )


def _read_initial(logs_dir) -> dict:
    with open(_get_initial_run_json_path(logs_dir), encoding="utf-8") as f:
        return json.load(f)


@contextlib.contextmanager
def _unlocked():
    """Runs the read-modify-write without flufl.lock, which is not needed single-threaded."""
    with mock.patch(
        "firexapp.reporters.json_reporter._run_json_lock",
        lambda _logs_dir: contextlib.nullcontext(),
    ):
        yield


class RunSoftTimeLimitInRunJsonTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp_dir.cleanup)
        self.logs_dir = self._tmp_dir.name

    def test_absent_until_a_task_raises_the_budget(self):
        _run_data(self.logs_dir).write_initial_run_json()

        self.assertIsNone(_read_initial(self.logs_dir)["run_soft_time_limit"])
        self.assertIsNone(FireXRunData.load_initial(self.logs_dir).run_soft_time_limit)

    def test_a_raise_is_recorded(self):
        _run_data(self.logs_dir).write_initial_run_json()

        with _unlocked():
            recorded = FireXRunData.persist_run_soft_time_limit(
                self.logs_dir, 60 * 60 * 60
            )

        self.assertEqual(recorded, 60 * 60 * 60)
        self.assertEqual(
            FireXRunData.load_initial(self.logs_dir).run_soft_time_limit,
            60 * 60 * 60,
        )

    def test_a_raise_leaves_the_rest_of_run_json_alone(self):
        _run_data(self.logs_dir).write_initial_run_json()
        before = _read_initial(self.logs_dir)

        with _unlocked():
            FireXRunData.persist_run_soft_time_limit(self.logs_dir, 60 * 60 * 60)

        after = _read_initial(self.logs_dir)
        self.assertEqual(after["run_soft_time_limit"], 60 * 60 * 60)
        del before["run_soft_time_limit"], after["run_soft_time_limit"]
        self.assertEqual(before, after)

    def test_fields_this_class_does_not_model_survive_a_raise(self):
        # Bundles subclass FireXRunData; the raise is written by the base class, which
        # must not drop what the subclass added.
        _run_data(self.logs_dir).write_initial_run_json()
        raw = _read_initial(self.logs_dir) | {"some_bundle_field": ["a", "b"]}
        with open(
            _get_initial_run_json_path(self.logs_dir), "w", encoding="utf-8"
        ) as f:
            json.dump(raw, f)

        with _unlocked():
            FireXRunData.persist_run_soft_time_limit(self.logs_dir, 60 * 60 * 60)

        self.assertEqual(_read_initial(self.logs_dir)["some_bundle_field"], ["a", "b"])

    def test_a_smaller_raise_does_not_lower_the_recorded_budget(self):
        _run_data(self.logs_dir).write_initial_run_json()

        with _unlocked():
            FireXRunData.persist_run_soft_time_limit(self.logs_dir, 60 * 60 * 60)
            recorded = FireXRunData.persist_run_soft_time_limit(
                self.logs_dir, 50 * 60 * 60
            )

        self.assertEqual(recorded, 60 * 60 * 60)
        self.assertEqual(
            FireXRunData.load_initial(self.logs_dir).run_soft_time_limit,
            60 * 60 * 60,
        )

    def test_the_completion_report_picks_up_a_raise_the_submit_process_never_saw(self):
        # The submit process holds its FireXRunData for the whole run, so its copy still
        # says None; the raise happened in a worker.
        submit_proc_run_data = _run_data(self.logs_dir)
        submit_proc_run_data.write_initial_run_json()

        with _unlocked():
            FireXRunData.persist_run_soft_time_limit(self.logs_dir, 60 * 60 * 60)
        self.assertIsNone(submit_proc_run_data.run_soft_time_limit)

        submit_proc_run_data.write_run_completed(results={})

        completed = FireXRunData.load_from_logs_dir(self.logs_dir)
        self.assertTrue(completed.completed)
        self.assertEqual(completed.run_soft_time_limit, 60 * 60 * 60)

    def test_writing_input_args_does_not_clobber_a_raise(self):
        submit_proc_run_data = _run_data(self.logs_dir)
        submit_proc_run_data.write_initial_run_json()

        with _unlocked():
            FireXRunData.persist_run_soft_time_limit(self.logs_dir, 60 * 60 * 60)

        submit_proc_run_data.write_update_input_args({"chain": "nop", "sleep": 1})

        reloaded = FireXRunData.load_initial(self.logs_dir)
        self.assertEqual(reloaded.run_soft_time_limit, 60 * 60 * 60)
        self.assertEqual(reloaded.inputs, {"sleep": 1})

    def test_a_missing_run_json_does_not_fail_the_completion_write(self):
        # Nothing was ever written to logs_dir, so the refresh read finds no file.
        run_data = _run_data(self.logs_dir)

        run_data.write_run_completed(results={})

        self.assertTrue(FireXRunData.load_from_logs_dir(self.logs_dir).completed)

    def test_the_read_modify_write_is_done_under_the_lock(self):
        # The requests come from arbitrary worker hosts, so the atomic replace that makes
        # each write indivisible is not enough on its own.
        _run_data(self.logs_dir).write_initial_run_json()

        entered_holding_lock = None

        @contextlib.contextmanager
        def spy_lock(logs_dir):
            self.assertEqual(logs_dir, self.logs_dir)
            nonlocal entered_holding_lock
            entered_holding_lock = False
            yield
            entered_holding_lock = True

        with (
            mock.patch(
                "firexapp.reporters.json_reporter._run_json_lock",
                spy_lock,
            ),
            mock.patch(
                "firexapp.reporters.json_reporter._write_run_json",
            ) as write_run_json,
        ):
            write_run_json.side_effect = lambda *a, **k: self.assertIs(
                entered_holding_lock, False, "run.json written outside the lock"
            )
            FireXRunData.persist_run_soft_time_limit(self.logs_dir, 60 * 60 * 60)

        write_run_json.assert_called_once()
        self.assertIs(entered_holding_lock, True, "the lock was never released")


class RefreshRunSoftTimeLimitTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp_dir.cleanup)
        self.logs_dir = self._tmp_dir.name

    def test_a_larger_in_memory_budget_is_not_replaced_by_a_smaller_recorded_one(self):
        on_disk = _run_data(self.logs_dir, run_soft_time_limit=50 * 60 * 60)
        _write_run_json(on_disk, _get_initial_run_json_path(self.logs_dir))

        in_memory = _run_data(self.logs_dir, run_soft_time_limit=60 * 60 * 60)
        in_memory._refresh_run_soft_time_limit()

        self.assertEqual(in_memory.run_soft_time_limit, 60 * 60 * 60)

    def test_a_corrupt_run_json_is_survivable(self):
        os.makedirs(os.path.dirname(_get_initial_run_json_path(self.logs_dir)))
        with open(
            _get_initial_run_json_path(self.logs_dir), "w", encoding="utf-8"
        ) as f:
            f.write("{not json")

        run_data = _run_data(self.logs_dir, run_soft_time_limit=60 * 60 * 60)
        run_data._refresh_run_soft_time_limit()

        self.assertEqual(run_data.run_soft_time_limit, 60 * 60 * 60)


if __name__ == "__main__":
    unittest.main()
