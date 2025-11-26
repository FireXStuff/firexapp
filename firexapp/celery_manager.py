from firexapp.submit.console import setup_console_logging
from logging import DEBUG
import os
import re
import subprocess
from typing import Optional, Union
import pathlib


from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.common import poll_until_file_not_empty
from firexapp.plugins import PLUGINS_ENV_NAME
from collections.abc import Iterable
from firexapp.common import qualify_firex_bin
from firexkit import firex_worker
import firexapp.firex_subprocess

logger = setup_console_logging(__name__)


class CeleryWorkerStartFailed(Exception):
    pass


class CeleryManager(object):

    @classmethod
    def log(cls, msg, level=DEBUG):
        logger.log(level, f'[{cls.__name__}] {msg}')

    @staticmethod
    def get_worker_logs_dir(logs_dir):
        return os.path.join(logs_dir, 'microservice_logs')

    @classmethod
    def start_celery(
        cls,
        worker_id: firex_worker.FireXWorkerId,
        app: str,
        logs_dir: str,
        queues: str,
        plugins: Union[str, list[str]],
        wait=True,
        concurrency=None,
        worker_log_level='debug',
        cap_concurrency=None,
        cwd=None,
        soft_time_limit=None,
        autoscale: Optional[tuple]=None,
        detach: bool=True,
        env=None,
        timeout=15*60,
    ) -> firex_worker.RunWorkerId:
        if concurrency and autoscale:
            raise AssertionError('You can either provide a value of concurrency or autoscale, but not both')

        # confirm broker URL set in env.
        BrokerFactory.get_broker_url(assert_if_not_set=True)

        run_worker_id = firex_worker.RunWorkerId(worker_id=worker_id, logs_dir=logs_dir)
        worker_log_path = pathlib.Path(cls.get_worker_logs_dir(logs_dir), f"{worker_id}.html")
        worker_log_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = (
            f'{qualify_firex_bin("celery")} '
            f'--app={app} worker '
            f'--hostname={worker_id} '
            f'--loglevel={worker_log_level} '
            f'--logfile={worker_log_path} '
            f'{run_worker_id.get_pidfile_cmd_part(create_parent_dir=True)} '
            f'--events '
            f'--without-gossip '
            f'--without-heartbeat '
            f'--without-mingle '
            f'-Ofair'
        )
        if queues:
            cmd += f' --queues={queues}'

        if concurrency:
            cmd += f' --concurrency={_cap_cpu_count(concurrency, cap_concurrency)}'
        elif autoscale:
            assert isinstance(autoscale, Iterable), 'autoscale should be a tuple of (min, max)'
            assert len(autoscale) == 2, 'autoscale should be a tuple of two elements (min, max)'
            autoscale_v1, autoscale_v2 = autoscale
            autoscale_min = _cap_cpu_count(min(autoscale_v1, autoscale_v2), cap_concurrency)
            autoscale_max = _cap_cpu_count(max(autoscale_v1, autoscale_v2), cap_concurrency)
            cmd += f' --autoscale={autoscale_max},{autoscale_min}'

        if soft_time_limit:
            cmd += f' --soft-time-limit={soft_time_limit}'

        if detach:
            cmd += ' &'

        stdout_path = run_worker_id.get_stdout_path()
        stdout_path.parent.mkdir(exist_ok=True, parents=True)

        extra_env = {
            'CELERY_RDBSIG': '1',
            PLUGINS_ENV_NAME: plugins if isinstance(plugins, str) else ",".join(plugins),
        } | (env or {})
        cls.log(f'Adding env to {worker_id} Celery worker: {extra_env}')

        cls.log(f'Starting {worker_id.prefix_queue()} on {worker_id.hostname} ...')
        firexapp.firex_subprocess.run(
            cmd,
            shell=True,
            file=str(stdout_path),
            stderr=subprocess.STDOUT,
            env=os.environ | extra_env,
            cwd=cwd,
            file_mode='ab+',
            check=True,
        )

        if detach and wait:
            _wait_until_active(
                pid_file=str(run_worker_id.get_pid_file()),
                timeout=timeout,
                stdout_file=str(stdout_path),
                worker_id=worker_id)
            cls.log(f'pid {run_worker_id.get_pid()} became active')

        return run_worker_id


def _extract_errors_from_celery_logs(celery_log_file, max_errors=20):
    err_list = None
    try:
        with open(celery_log_file, encoding='ascii', errors='ignore') as f:
            logs = f.read()
            err_list = re.findall(r'^\S*Error: .*$', logs, re.MULTILINE)
            if err_list:
                err_list = err_list[0:max_errors]
    except FileNotFoundError:
        pass

    return err_list


def _cap_cpu_count(count, cap_concurrency):
    return min(count, cap_concurrency) if cap_concurrency else count


def _wait_until_active(pid_file: str, stdout_file: str, worker_id: firex_worker.FireXWorkerId, timeout: int):
    try:
        poll_until_file_not_empty(pid_file, timeout=timeout)
    except AssertionError:
        err_list = _extract_errors_from_celery_logs(stdout_file)
        extra_err_info = ''
        if err_list:
            extra_err_info += '\nFound the following errors:\n' + '\n'.join(err_list)

        extra_err_info += '\nAttempting to delete the invocation pids'
        deleted_pids = subprocess.run(
            ['/bin/pkill', '-e', '-f', pid_file],
            capture_output=True,
            check=False,
            text=True)

        if deleted_pids.stdout:
            extra_err_info += f'\nstdout: {deleted_pids.stdout}'
        if deleted_pids.stderr:
            extra_err_info += f'\nstderr: {deleted_pids.stderr}'

        raise CeleryWorkerStartFailed(
            f'The worker {worker_id} did not come up after'
            f' {timeout} seconds.\n'
            f'Please look into {stdout_file!r} for details.'
            f'{extra_err_info}') from None

