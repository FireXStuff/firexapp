import logging
from typing import List, Optional
import os
from tempfile import TemporaryDirectory
import shutil
import subprocess

from firexapp.events.model import TaskColumn, RunStates, FireXTask, is_chain_exception, get_chain_exception_child_uuid
from firex_keeper.db_model import firex_tasks
from firex_keeper.persist import get_db_manager, task_by_uuid_exp, get_keeper_complete_file_path, \
    get_db_file, get_keeper_query_ready_file_path
from firex_keeper.keeper_helper import FireXTreeTask
from firexapp.common import wait_until
from sqlalchemy.sql import and_, select
from sqlalchemy import literal

logger = logging.getLogger(__name__)


INCOMPLETE_RUNSTATES = {
    s.to_celery_event_type() for s in RunStates if not s.is_complete()
}
REVOKED_RUNSTATES = {
    s.to_celery_event_type() for s in [RunStates.REVOKED, RunStates.REVOKE_COMPLETED]
}

class FireXTaskQueryException(Exception):
    pass


def _task_col_eq(task_col, val):
    return firex_tasks.c[task_col.value] == val


def _wait_and_query(logs_dir, query, db_file_query_ready_timeout, **kwargs) -> List[FireXTask]:
    wait_on_keeper_query_ready(logs_dir, db_file_query_ready_timeout)
    with get_db_manager(logs_dir) as db_manager:
        return db_manager.query_tasks(query, **kwargs)


def _unlink_if_exists(path: str) -> None:
    try:
        if os.path.isfile(path):
            os.unlink(path)
    except OSError:
        pass


def _copy_keeper_db_for_local_query(existing_db_file: str, new_tmp_db_file: str, tmp_cwd: str) -> None:
    """Copy keeper SQLite to a temp path for local reads.
    try rsync (good over NFS / cross-geo), then sqlite3 .backup (consistent snapshot),
    then shutil.copyfile.
    """
    rsync_bin = shutil.which('rsync')
    if rsync_bin:
        try:
            subprocess.check_output(
                [rsync_bin, existing_db_file, new_tmp_db_file],
                cwd=tmp_cwd,
            )
            if os.path.isfile(new_tmp_db_file) and os.path.getsize(new_tmp_db_file) > 0:
                logger.info('Copied keeper DB for local query using rsync')
                return
            logger.warning(
                'rsync keeper copy succeeded but dest missing or empty: %s',
                new_tmp_db_file,
            )
        except (OSError, subprocess.SubprocessError) as e:
            logger.warning('rsync keeper copy failed: %s', e)
        _unlink_if_exists(new_tmp_db_file)

    sqlite_bin = '/bin/sqlite3'
    if os.path.isfile(sqlite_bin):
        try:
            subprocess.check_output(
                [sqlite_bin, existing_db_file, f'.backup {new_tmp_db_file}'],
                cwd=tmp_cwd,
            )
            logger.info('Copied keeper DB for local query using sqlite3 .backup')
            return
        except (OSError, subprocess.SubprocessError) as e:
            logger.warning('sqlite3 .backup keeper copy failed: %s', e)
        _unlink_if_exists(new_tmp_db_file)

    shutil.copyfile(existing_db_file, new_tmp_db_file)
    logger.info('Copied keeper DB for local query using shutil.copyfile')


def _query_tasks(logs_dir, query, db_file_query_ready_timeout=15, copy_before_query=False, **kwargs) -> List[FireXTask]:
    if copy_before_query:
        tmp_base_dir = '/dev/shm' if os.path.isdir('/dev/shm') else None
        with TemporaryDirectory(dir=tmp_base_dir) as temp_log_dir:
            existing_db_file = get_db_file(logs_dir, new=False)
            new_tmp_db_file = get_db_file(temp_log_dir, new=True)

            _copy_keeper_db_for_local_query(existing_db_file, new_tmp_db_file, temp_log_dir)

            query_results = _wait_and_query(temp_log_dir, query, db_file_query_ready_timeout, **kwargs)
    else:
        query_results = _wait_and_query(logs_dir, query, db_file_query_ready_timeout, **kwargs)
    return query_results


def all_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return _query_tasks(logs_dir, True, **kwargs)


def tasks_by_name(logs_dir, name, **kwargs) -> List[FireXTask]:
    if '.' in name:
        col = TaskColumn.LONG_NAME
    else:
        col = TaskColumn.NAME
    return _query_tasks(logs_dir, _task_col_eq(col, name), **kwargs)


def single_task_by_name(logs_dir, name, **kwargs) -> FireXTask:
    tasks = _query_tasks(logs_dir, _task_col_eq(TaskColumn.NAME, name), **kwargs)
    if len(tasks) != 1:
        raise FireXTaskQueryException("Required exactly one task named '%s', found %s" % (name, len(tasks)))
    return tasks[0]


def task_by_uuid(logs_dir, uuid, wait_for_exp_exist=None, max_wait=3, **kwargs) -> FireXTask:
    if wait_for_exp_exist is None:
        wait_for_exp_exist=task_by_uuid_exp(uuid)

    tasks = _query_tasks(
        logs_dir,
        _task_col_eq(TaskColumn.UUID, uuid),
        wait_for_exp_exist=wait_for_exp_exist,
        max_wait=max_wait,
        **kwargs)

    if not tasks:
        raise FireXTaskQueryException(f"Found no task with UUID {uuid}")
    return tasks[0]


def task_by_name_and_arg_pred(logs_dir, name, arg, pred) -> List[FireXTask]:
    tasks_with_name = tasks_by_name(logs_dir, name)
    return [t for t in tasks_with_name if arg in t.firex_bound_args and pred(t.firex_bound_args[arg])]


def task_by_name_and_arg_value(logs_dir, name, arg, value) -> List[FireXTask]:
    pred = lambda arg_value: arg_value == value
    return task_by_name_and_arg_pred(logs_dir, name, arg, pred)


def failed_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.STATE, RunStates.FAILED.value), **kwargs)


def revoked_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return _query_tasks(
        logs_dir,
        firex_tasks.c[TaskColumn.STATE.value].in_(REVOKED_RUNSTATES),
        **kwargs)


def running_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return _query_tasks(logs_dir, firex_tasks.c[TaskColumn.STATE.value].in_(INCOMPLETE_RUNSTATES), **kwargs)


def running_not_blocked_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return _query_tasks(
        logs_dir,
        and_(
            firex_tasks.c[TaskColumn.STATE.value].in_(INCOMPLETE_RUNSTATES),
            firex_tasks.c[TaskColumn.STATE.value] != RunStates.BLOCKED.value,
        ),
        **kwargs)

def failed_by_tasks(logs_dir, failed_uuid: str, **kwargs) -> List[FireXTask]:
    # TODO: make this work with copy_before_query without copying twice,
    # or tune page size to make NFS queries faster.
    # assert is_failed(task_by_uuid(logs_dir, failed_uuid, **kwargs)), \
    #     f'Task {failed_uuid} did not fail.'
    return _query_tasks(
        logs_dir,
        firex_tasks.c[TaskColumn.EXCEPTION_CAUSE_UUID.value] == failed_uuid,
        **kwargs)


def _child_ids_by_parent_id(tasks_by_uuid):
    child_uuids_by_parent_id = {u: [] for u in tasks_by_uuid.keys()}

    for t in tasks_by_uuid.values():
        # TODO: what if a child is entered in to the DB before its parent? Ignore for now.
        if t.parent_id in child_uuids_by_parent_id:
            child_uuids_by_parent_id[t.parent_id].append(t.uuid)

    return child_uuids_by_parent_id


def _get_tree_tasks_by_uuid(root_uuid, tasks_by_uuid):
    if root_uuid is None:
        root_uuid = next((t.uuid for t in tasks_by_uuid.values() if t.parent_id is None), None)
        # FIXME: handle multiple roots?
        if root_uuid is None:
            raise Exception("Found no root task with null parent_id.")

    child_ids_by_parent_id = _child_ids_by_parent_id(tasks_by_uuid)

    tree_tasks_by_uuid = {}
    if root_uuid in tasks_by_uuid:
        uuids_to_add = [root_uuid]
        while uuids_to_add:
            cur_task_uuid = uuids_to_add.pop()
            cur_task = tasks_by_uuid[cur_task_uuid]
            parent_tree_task = tree_tasks_by_uuid.get(cur_task.parent_id, None)

            cur_tree_task = FireXTreeTask(**{**cur_task._asdict(), 'children': [], 'parent': parent_tree_task})
            if parent_tree_task:
                parent_tree_task.children.append(cur_tree_task)
            tree_tasks_by_uuid[cur_tree_task.uuid] = cur_tree_task

            uuids_to_add += child_ids_by_parent_id[cur_tree_task.uuid]

    return tree_tasks_by_uuid


def _create_task_tree(logs_dir, root_uuid=None, **kwargs) -> Optional[FireXTreeTask]:
    with get_db_manager(logs_dir) as db_manager:
        if root_uuid is None:
            root_uuid = db_manager.query_single_run_metadata().root_uuid

        descendant_task_uuids = select(
            literal(root_uuid).label("uuid")
        ).cte("descendant_task_uuids", recursive=True)

        descendant_uuids = (
            select(firex_tasks.c.uuid)
            .join(
                descendant_task_uuids,
                firex_tasks.c.parent_id == descendant_task_uuids.c.uuid)
        )

        # Approx SQL, uses index on parent_id.
        # WITH RECURSIVE descendant_task_uuids(uuid) AS (
        #   SELECT :param_1 AS uuid
        #   UNION
        #   SELECT firex_tasks.uuid AS uuid
        #   FROM firex_tasks
        #   JOIN descendant_task_uuids ON firex_tasks.parent_id = descendant_task_uuids.uuid
        # )
        # SELECT firex_tasks.*
        # FROM firex_tasks
        # WHERE firex_tasks.uuid IN (
        #   SELECT uuid FROM descendant_task_uuids
        # )
        final_stmt = select(firex_tasks).where(
            firex_tasks.c.uuid.in_(
                select(
                    descendant_task_uuids.union(descendant_uuids))
            )
        )

        root_and_descendant_tasks = db_manager.query_tasks(final_stmt, **kwargs)

    return _get_tree_tasks_by_uuid(
        root_uuid,
        {t.uuid: t for t in root_and_descendant_tasks},
    ).get(root_uuid) # root_uuid might not be in task table.


def task_tree_to_task(task_tree: FireXTreeTask) -> FireXTask:
    task_tree_dict = task_tree._asdict()
    task_tree_dict.pop('children')
    task_tree_dict.pop('parent')
    return FireXTask(**task_tree_dict)


def flatten_tree(task_tree: FireXTreeTask) -> List[FireXTreeTask]:
    flat_tasks = []
    to_check = [task_tree]
    while to_check:
        cur_task = to_check.pop()
        to_check += cur_task.children
        flat_tasks.append(cur_task)

    return flat_tasks


def get_descendants(logs_dir, uuid) -> List[FireXTreeTask]:
    # TODO: historically a FireXTreeTask was returned because
    # the graph needed to be created within the application anyways.
    # Now that sqlite recursive query is managing the tree,
    # it's no longer necessary to create the graph in memory
    # unless the caller actually needs it, which many currently don't
    # (i.e. most call sites don't use "children" or "parent")
    # Therefore an optimization is possible to query descendants
    # but return a FireXTask, skipping application-side tree creation.

    subtree = _create_task_tree(logs_dir, root_uuid=uuid)
    # None if UUID isn't found.
    if subtree is None:
        return []
    return [t for t in flatten_tree(subtree) if t.uuid != uuid]


def ancestor_by_long_name(logs_dir, uuid, ancestor_long_name, **kwargs) -> FireXTreeTask:
    tasks_by_uuid = {t.uuid: t for t in all_tasks(logs_dir, **kwargs)}
    # TODO: could avoid fetching all tasks by using sqlite recursive query.
    tree_tasks_by_uuid = _get_tree_tasks_by_uuid(None, tasks_by_uuid)
    if uuid in tree_tasks_by_uuid:
        tree_task = tree_tasks_by_uuid[uuid]
        while tree_task.parent and tree_task.parent.long_name != ancestor_long_name:
            tree_task = tree_task.parent
        if tree_task.parent.long_name == ancestor_long_name:
            return tree_task.parent
    return None


def find_task_causing_chain_exception(task: FireXTreeTask):
    assert task.exception, "Expected exception, received: %s" % task.exception

    if not is_chain_exception(task) or not task.children:
        return task

    causing_uuid = get_chain_exception_child_uuid(task)
    causing_child = [c for c in task.children if c.uuid == causing_uuid]

    # Note a chain interrupted exception can be caused by a non-descendant task via stitch_chains.
    if not causing_child:
        return task
    causing_child = causing_child[0]

    if not is_chain_exception(causing_child):
        return causing_child

    return find_task_causing_chain_exception(causing_child)


def wait_on_keeper_query_ready(logs_dir: str, timeout: int=10):
    if os.path.isfile(get_keeper_complete_file_path(logs_dir)):
        return True
    return wait_until(os.path.isfile, timeout, 0.5,
                      get_keeper_query_ready_file_path(logs_dir))


def wait_on_keeper_complete(logs_dir, timeout=30) -> bool:
    return wait_until(os.path.isfile, timeout, 1,
                      get_keeper_complete_file_path(logs_dir))