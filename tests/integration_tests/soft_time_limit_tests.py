import os
from time import monotonic, sleep

from celery.exceptions import SoftTimeLimitExceeded

from firexapp.engine.celery import app
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run
from firexkit.run_time import RunTimeReserve

# The soft_time_limit the children are submitted with, and the (much larger) one
# the parents raise it to while the children are running.
ORIGINAL_SOFT_TIME_LIMIT = 20
EXTENDED_SOFT_TIME_LIMIT = 120

# Only relevant to the clamping config; deliberately below EXTENDED_SOFT_TIME_LIMIT.
HARD_TIME_LIMIT = 30


def _write_marker(logs_dir, name, content="1"):
    with open(os.path.join(logs_dir, f"stl_{name}"), "w") as f:
        f.write(str(content))


def _assert_marker(logs_dir, name, exists=True):
    marker = os.path.join(logs_dir, f"stl_{name}")
    if exists:
        assert os.path.isfile(marker), f"Expected marker file '{marker}' to exist."
    else:
        assert not os.path.isfile(marker), (
            f"Expected marker file '{marker}' to be absent."
        )


def _wait_for_marker(logs_dir, name, max_wait=90):
    give_up_at = monotonic() + max_wait
    while monotonic() < give_up_at:
        if os.path.isfile(os.path.join(logs_dir, f"stl_{name}")):
            return
        sleep(1)
    raise AssertionError(f"Marker '{name}' was never written.")


def _wait_for_child_prerun(task, child_id, max_wait=90):
    give_up_at = monotonic() + max_wait
    while monotonic() < give_up_at:
        if task.app.task_id_has_prerun(child_id):
            return
        sleep(1)
    raise AssertionError(f"Child task {child_id} never started running.")


@app.task(bind=True, soft_time_limit=ORIGINAL_SOFT_TIME_LIMIT)
def sleep_past_original_soft_time_limit(self, uid, sleep_for):
    try:
        sleep(sleep_for)
    except SoftTimeLimitExceeded:
        _write_marker(uid.logs_dir, "soft_time_limit_exceeded")
        raise
    _write_marker(uid.logs_dir, "completed")


@app.task(bind=True)
def extend_running_child_soft_time_limit(self, uid):
    # Sleeps for over twice the soft_time_limit it's submitted with, so it can only
    # complete if the extension applied while it was already running.
    child = self.enqueue_child(
        sleep_past_original_soft_time_limit.s(
            uid=uid,
            sleep_for=ORIGINAL_SOFT_TIME_LIMIT * 2.5,
        ),
    )
    _wait_for_child_prerun(self, child.id)

    applied = self.app.set_task_soft_time_limit(child.id, EXTENDED_SOFT_TIME_LIMIT)
    assert applied == EXTENDED_SOFT_TIME_LIMIT, (
        f"Expected the worker to apply a soft_time_limit of"
        f" {EXTENDED_SOFT_TIME_LIMIT}, got: {applied}"
    )

    self.wait_for_children()


class SoftTimeLimitExtendedForRunningTask(FlowTestConfiguration):
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "extend_running_child_soft_time_limit"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = self.run_data.logs_path
        _assert_marker(logs_dir, "soft_time_limit_exceeded", exists=False)
        _assert_marker(logs_dir, "completed")

    def assert_expected_return_code(self, ret_value):
        assert ret_value == 0, f"Expected a successful run, got: {ret_value}"


@app.task(
    bind=True,
    soft_time_limit=ORIGINAL_SOFT_TIME_LIMIT,
    time_limit=HARD_TIME_LIMIT,
)
def sleep_past_hard_time_limit(self, uid, sleep_for):
    try:
        sleep(sleep_for)
    except SoftTimeLimitExceeded:
        _write_marker(uid.logs_dir, "soft_time_limit_exceeded")
        raise
    _write_marker(uid.logs_dir, "completed")


@app.task(bind=True)
def clamp_running_child_soft_time_limit(self, uid):
    child = self.enqueue_child(
        sleep_past_hard_time_limit.s(uid=uid, sleep_for=HARD_TIME_LIMIT * 3),
    )
    _wait_for_child_prerun(self, child.id)

    # The hard time_limit is never moved, so the request is clamped down to it.
    applied = self.app.set_task_soft_time_limit(child.id, EXTENDED_SOFT_TIME_LIMIT)
    _write_marker(uid.logs_dir, "applied", content=applied)

    self.wait_for_children()


class SoftTimeLimitClampedToHardTimeLimit(FlowTestConfiguration):
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "clamp_running_child_soft_time_limit"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = self.run_data.logs_path
        _assert_marker(logs_dir, "applied")
        with open(os.path.join(logs_dir, "stl_applied")) as f:
            applied = f.read()
        assert float(applied) == HARD_TIME_LIMIT, (
            f"Expected the requested soft_time_limit to be clamped to the hard"
            f" time_limit of {HARD_TIME_LIMIT}, got: {applied}"
        )
        _assert_marker(logs_dir, "completed", exists=False)

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


# The run's total time budget, as submitted. Everything below deliberately needs more
# than this, and only survives because the budget is raised while the run is going.
RUN_SOFT_TIME_LIMIT = 40
# Longer than the whole original budget, and longer than the floor a RunTimeReserve
# resolves to, so nothing but a real increase can keep this task alive.
LEAF_SLEEP = 75
# The reserve the leaf keeps back for the cleanup and post-processing it triggers.
LEAF_RESERVE = 30
# Run by a task enqueued after the raise, which declares no reserve at all: it survives
# only if the worker's default task soft time limit followed the budget up.
POST_RAISE_SLEEP = 50


@app.task(bind=True, run_time_limit_reserve=LEAF_RESERVE)
def sleep_past_the_original_run_budget(self, uid):
    # Not raising until the sibling waiter is blocked is what makes this a test of the
    # running-task path: otherwise the sibling could be dispatched after the raise and
    # pick up the new worker default, which proves nothing.
    _wait_for_marker(uid.logs_dir, "sibling_waiter_blocked")
    # Its limit was resolved at publish from a budget too small for what it now knows it
    # needs, so it raises the run's budget before doing the work.
    self.ensure_run_time_remaining(LEAF_SLEEP * 2, reserve=LEAF_RESERVE)
    try:
        sleep(LEAF_SLEEP)
    except SoftTimeLimitExceeded:
        _write_marker(uid.logs_dir, "leaf_soft_time_limit_exceeded")
        raise
    _write_marker(uid.logs_dir, "leaf_completed")


@app.task(bind=True)
def wait_on_the_long_leaf(self, uid):
    # Already blocked when the budget is raised: this wait has to move with it, or the
    # leaf outlives the parent waiting on it and the run fails anyway.
    self.enqueue_child(
        sleep_past_the_original_run_budget.s(uid=uid),
        block=True,
        max_wait=RunTimeReserve(5),
    )
    _write_marker(uid.logs_dir, "blocked_ancestor_wait_returned")


@app.task(bind=True)
def sleep_past_the_original_budget_undeclared(self, uid):
    try:
        sleep(POST_RAISE_SLEEP)
    except SoftTimeLimitExceeded:
        _write_marker(uid.logs_dir, "post_raise_soft_time_limit_exceeded")
        raise
    _write_marker(uid.logs_dir, "post_raise_completed")


@app.task(bind=True)
def wait_on_a_sibling_branch(self, uid, waiton):
    # Handed a result for a branch it did not enqueue, so it is not an ancestor of the
    # leaf that raises the budget -- no walk up the enqueue hierarchy reaches it. It is
    # still blocked on work that now runs longer, and it declared no limit of its own,
    # so the raise has to reach it through the worker default it was already following.
    _write_marker(uid.logs_dir, "sibling_waiter_blocked")
    waiton.fx_wait(max_wait=RunTimeReserve(5), parent_id=self.request.id)
    _write_marker(uid.logs_dir, "sibling_waiter_returned")


@app.task(bind=True)
def raise_run_budget_from_a_descendant(self, uid):
    branch = self.enqueue_child(wait_on_the_long_leaf.s(uid=uid))
    self.enqueue_child(
        wait_on_a_sibling_branch.s(uid=uid, waiton=branch),
        block=True,
    )
    self.wait_for_children()
    # Enqueued after the raise, so it should get the larger worker default.
    self.enqueue_child(sleep_past_the_original_budget_undeclared.s(uid=uid), block=True)


class RunSoftTimeLimitRaisedByDescendant(FlowTestConfiguration):
    no_coverage = True

    def initial_firex_options(self) -> list:
        return [
            "submit",
            "--chain",
            "raise_run_budget_from_a_descendant",
            "--soft_time_limit",
            str(RUN_SOFT_TIME_LIMIT),
        ]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = self.run_data.logs_path
        _assert_marker(logs_dir, "leaf_soft_time_limit_exceeded", exists=False)
        _assert_marker(logs_dir, "leaf_completed")
        _assert_marker(logs_dir, "blocked_ancestor_wait_returned")
        _assert_marker(logs_dir, "sibling_waiter_returned")
        _assert_marker(logs_dir, "post_raise_soft_time_limit_exceeded", exists=False)
        _assert_marker(logs_dir, "post_raise_completed")

    def assert_expected_return_code(self, ret_value):
        assert ret_value == 0, f"Expected a successful run, got: {ret_value}"
