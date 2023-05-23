import datetime
from enum import Enum, auto
import logging
import os
import shlex
import shutil
from socket import gethostname
import stat
import subprocess
import tempfile
import time
import urllib.parse
import uuid
from typing import Union

from celery.utils.log import get_task_logger

from firexkit import firex_exceptions, firexkit_common
from firexapp.engine.logging import html_escape
from firexapp.events.model import EXTERNAL_COMMANDS_KEY

logger = get_task_logger(__name__)


class CommandFailed(firex_exceptions.FireXCalledProcessError):
    pass


class _SubprocessRunnerType(Enum):
    CHECK_OUTPUT = auto()
    CHECK_CALL = auto()
    RUN = auto()


def open_og_rw_permissions(path):
    rw_group = stat.S_IRGRP | stat.S_IWGRP
    rw_other = stat.S_IROTH | stat.S_IWOTH
    os.chmod(path, os.stat(path).st_mode | rw_group | rw_other)


def check_call(cmd, retries=0, retry_delay=3, **kwargs):
    runner_type = _SubprocessRunnerType.CHECK_CALL

    _sanitize_runner_kwargs(runner_type=runner_type, kwargs=kwargs)
    _subprocess_runner_retries(runner_type=runner_type, capture_output=False, check=True,
                               retries=retries, retry_delay=retry_delay, cmd=cmd, **kwargs)


def check_output(cmd, retries=0, retry_delay=3, **kwargs):
    runner_type = _SubprocessRunnerType.CHECK_OUTPUT

    _sanitize_runner_kwargs(runner_type=runner_type, kwargs=kwargs)
    result = _subprocess_runner_retries(runner_type=runner_type, capture_output=True, check=True,
                                        retries=retries, retry_delay=retry_delay, cmd=cmd, **kwargs)
    return result.stdout


def run(cmd, retries=0, retry_delay=3, **kwargs):
    # Note that this differs from Python 3.8's API since capture_output is True by default in _subprocess_runner

    runner_type = _SubprocessRunnerType.RUN

    _sanitize_runner_kwargs(runner_type=runner_type, kwargs=kwargs)
    return _subprocess_runner_retries(runner_type=runner_type,  # Pass through 'capture_output' and 'check'
                                      retries=retries, retry_delay=retry_delay, cmd=cmd, **kwargs)


def _sanitize_runner_kwargs(runner_type: _SubprocessRunnerType, kwargs: dict):
    disallowed_keys = ['stdout', 'universal_newlines']
    if runner_type is _SubprocessRunnerType.RUN:
        disallowed_keys.extend(['input'])
    else:
        disallowed_keys.extend(['capture_output', 'check'])

    for key in disallowed_keys:
        if key in kwargs:
            logger.error('[%s] WARNING: %s argument not allowed, it will be overridden.' %
                         (runner_type.name.lower(), key))
            del kwargs[key]


def _subprocess_runner_retries(retries, retry_delay, **kwargs):
    if retries is None:
        retries = 0
    if retry_delay is None:
        retry_delay = 0
    retry_count = 0
    while True:
        try:
            extra_header = '[retry %d/%d]' % (retry_count, retries) if retry_count else None
            return _subprocess_runner(extra_header=extra_header, **kwargs)
        except Exception:
            if retry_count >= retries:
                raise
            else:
                retry_count += 1
                time.sleep(retry_delay)


def _send_flame_subprocess(subprocess_data):
    try:
        from celery import current_task
        if current_task:
            current_task.send_firex_event_raw({EXTERNAL_COMMANDS_KEY: subprocess_data})
    except Exception as e:
        logger.warning(f"Error while sending flame subprocess event: {e}")


def _send_flame_subprocess_start(flame_subprocess_id, cmd, cwd, host, filename=None, remote_host=None):
    _send_flame_subprocess({flame_subprocess_id: {'cmd': cmd,
                                                  'cwd': str(cwd) if cwd else cwd,
                                                  'output_file': filename,
                                                  'host': host,
                                                  'remote_host': remote_host,
                                                  'start_time': time.time()}})


def _send_flame_subprocess_end(flame_subprocess_id, output, returncode, chars=None, hung_process=None,
                               slow_process=None, stderr_output=None):
    ui_max_chars = 8000
    chars = ui_max_chars if chars is None else min(chars, ui_max_chars)
    subproc_result = {
        'completed': not hung_process and not slow_process and returncode is not None,
        'timeout': slow_process,
        'inactive': hung_process,
        'output_truncated': len(output) >= chars if output is not None else False,
        'output': output[-chars:] if output else output,
        'stderr_output_truncated': len(stderr_output) >= chars if stderr_output is not None else False,
        'stderr_output': stderr_output[-chars:] if stderr_output else stderr_output,
        'returncode': returncode,
    }
    _send_flame_subprocess({flame_subprocess_id: {'result': subproc_result, 'end_time': time.time()}})


def _subprocess_runner(cmd: Union[str, list], runner_type: _SubprocessRunnerType = _SubprocessRunnerType.CHECK_OUTPUT,
                       extra_header=None, file=None, chars=32000, timeout=None, capture_output=True, check=False,
                       inactivity_timeout=30 * 60, log_level=logging.DEBUG, copy_file_path=None, shell=False, cwd=None,
                       env=None, remove_firex_pythonpath=True, logger=logger, stderr=subprocess.STDOUT, **kwargs):
    ##########################
    # Local Helper functions #
    ##########################
    def _get_cmd_str():
        if isinstance(cmd, list):
            return ' '.join([shlex.quote(token) for token in cmd])
        else:
            return cmd

    def _sanitize_cmd():
        nonlocal cmd
        if isinstance(cmd, str) and not shell:
            cmd = shlex.split(cmd)

    def _get_env_without_pythonpath(user_env):
        try:
            current_env_pythonpath = os.environ['PYTHONPATH']
        except KeyError:
            # if we don't have a PYTHONPATH in the current env, then we can't do anything
            return user_env
        else:
            if user_env is None:
                # User didn't supply an env, so, we remove our PYTHONPATH
                current_env_copy = os.environ.copy()
                del current_env_copy['PYTHONPATH']
                return current_env_copy
            else:
                # User provided a custom env
                try:
                    user_env_pythonpath = user_env['PYTHONPATH']
                except KeyError:
                    # If user provided custom env didn't have PYTHONPATH, then use as-is
                    return user_env
                else:
                    # Remove our PYTHONPATH from the user's env PYTHONPATH
                    current_env_pythonpath_items = current_env_pythonpath.split(':')
                    user_env_pythonpath_items = user_env_pythonpath.split(':')
                    new_pythonpath = [x for x in user_env_pythonpath_items if (x not in current_env_pythonpath_items)]

                    user_env_copy = dict(user_env)
                    user_env_copy['PYTHONPATH'] = ':'.join(new_pythonpath)
                    return user_env_copy

    def _log_intro_msg(subprocess_uuid):
        msg = [f'{log_header} Executing the following command:']
        sep = '$' if shell else '>'
        live_link = _get_live_file_monitor_link()
        if live_link is None:
            live_link = ''
        msg += [f'{live_link}'
                f'<span class="command_line_prefix">{host}:{cwd_str}{sep}</span> '
                f'<span class="command_line">{html_escape(cmd_str)}</span>']
        span_class = 'command_extra_info' if file else 'hidden'
        msg += [f'<span class="{span_class}">output also written to: {filename}</span>']
        logger.log(log_level, '\n'.join(msg), extra={'label': subprocess_uuid,
                                                     'span_class': 'command',
                                                     'html_escape': False})

    def _get_live_file_monitor_link():
        from firexapp.engine.celery import app
        try:
            # FIXME: https://github.com/FireXStuff/firexapp/issues/10
            if app.conf.install_config.has_viewer():
                run_url = app.conf.install_config.get_run_url()
            else:
                return
        except AttributeError:
            return

        # Note we can't use urllib.parse.urljoin because the path is in the fragment, which urljoin can only overwrite.
        # FIXME: ideally flame-ui URL path knowledge would be externalized.
        if not run_url.endswith('/'):
            run_url += '/'
        live_link_url = run_url + f"live-file?file={urllib.parse.quote(filename, safe='')}&host={host}"
        link = firexkit_common.get_link(live_link_url,
                                        text='',
                                        title_attribute=f'Live monitor of {filename}',
                                        attrs={"target": "_blank", "style": "margin-right:5px"},
                                        other_elements='<i class="far fa-eye"></i>',
                                        html_class=live_file_monitor_span_class)
        return link

    def _hide_live_file_monitor_element():
        logger.raw('<style>.%s {display: none;}</style>' % live_file_monitor_span_class)

    def _get_output_from_file():
        if not file or not chars or os.fstat(f.fileno()).st_size < chars:
            f.seek(0)
        else:
            f.seek(-chars, 2)

        return f.read().decode(encoding='utf-8', errors='ignore')

    def _log_output(contents, error=False):
        if not chars or len(contents) < chars:
            log_msg = '%s Returned String:\n%s' % (log_header, contents)
        else:
            log_msg = '%s Last %d chars of Returned String:\n%s' % (log_header, chars, contents[-chars:])
        logger.log(log_level, log_msg.encode(encoding='ascii', errors='namereplace').decode(errors='ignore'),
                   extra={'span_class': 'command_output command_output_error' if error else 'command_output'})

    def _kill_proc_gently(proc):
        try:
            proc.terminate()
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                # Okay, kill not so gently
                proc.kill()
                try:
                    proc.wait(timeout=6)
                except subprocess.TimeoutExpired as e:
                    # Give up at this point. It is undead.
                    logger.exception(e)
                    pass

        except PermissionError as e:  # Possible if the underlying process is running under sudo or the like
            logger.exception(e)

    #################
    # Start of code #
    #################
    log_header = f'[{runner_type.name.lower()}]'
    log_header += extra_header if extra_header else ''
    cmd_str = _get_cmd_str()
    cwd_str = cwd if cwd is not None else os.getcwd()
    host = gethostname()
    subprocess_id = str(uuid.uuid4())
    live_file_monitor_span_class = f'live_file_{subprocess_id}'

    _sanitize_cmd()
    if remove_firex_pythonpath:
        env = _get_env_without_pythonpath(env)

    if file:
        f = open(file, 'wb+')
    else:
        f = tempfile.NamedTemporaryFile(delete=False)
    filename = f.name
    open_og_rw_permissions(filename)

    if log_level is not None:
        _send_flame_subprocess_start(flame_subprocess_id=subprocess_id, cmd=cmd, filename=filename, cwd=cwd_str,
                                     host=host)
        _log_intro_msg(subprocess_id)

    slow_process = hung_process = False
    with f:
        p = output = None
        try:
            # Run the command
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=f, stderr=stderr, shell=shell, cwd=cwd,
                                 env=env, **kwargs)
            start_time = time.monotonic()

            last_output_size = 0
            last_log_time = last_output_time = start_time
            last_output_clock_time = time.time()
            _sleep = 0.05
            # Wait for process to finish
            while True:
                try:
                    p.wait(_sleep)
                    # If we get here, process is done
                    break
                except subprocess.TimeoutExpired:
                    pass
                _sleep = _sleep * 1.1 if _sleep * 1.1 < 1 else 1  # Exponential backoff

                now = time.monotonic()
                if now - last_log_time > 10 * 60:  # Log every 10 minutes
                    time_str = datetime.datetime.fromtimestamp(last_output_clock_time).strftime('%Y-%m-%d %H:%M:%S')
                    logger.info(f'Waiting for command to finish...\n(Last command output was at {time_str}. '
                                f'Total output size is {last_output_size} bytes.)')
                    last_log_time = now

                if timeout and now - start_time > timeout:
                    # Process too slow. Kill it.
                    logger.debug(f'Total timeout {timeout} exceeded, killing pid {p.pid}')
                    _kill_proc_gently(p)
                    slow_process = True
                    break

                current_output_size = os.fstat(f.fileno()).st_size
                if last_output_size != current_output_size:
                    # We have some activity!
                    last_output_size = current_output_size
                    last_output_time = now
                    last_output_clock_time = time.time()
                elif inactivity_timeout and now - last_output_time > inactivity_timeout:
                    # Process hung. Kill it.
                    logger.debug(f'Activity (writing to stdout/stderr) timeout {inactivity_timeout} exceeded, killing pid {p.pid}')
                    _kill_proc_gently(p)
                    hung_process = True
                    break

            if capture_output:
                output = _get_output_from_file()
                if log_level is not None:
                    _log_output(output, error=True if p.returncode else False)

            # Should these exceptions be thrown after all the cleanups below?
            if slow_process:
                raise subprocess.TimeoutExpired(cmd, timeout, output=output, stderr=output)
            elif hung_process:
                raise firex_exceptions.FireXInactivityTimeoutExpired(cmd, inactivity_timeout, output=output,
                                                                     stderr=output)
        finally:
            if log_level is not None:
                _send_flame_subprocess_end(flame_subprocess_id=subprocess_id, hung_process=hung_process,
                                           slow_process=slow_process, output=output, chars=chars,
                                           returncode=getattr(p, 'returncode', None))
                _hide_live_file_monitor_element()

            if p and p.stdin:
                p.stdin.close()

            # Copy output file
            if (copy_file_path is not None and
                    not os.path.isfile(copy_file_path) and
                    os.path.isfile(filename)):
                shutil.copyfile(filename, copy_file_path)

    # Attempt to remove the temporary file
    if not file:
        try:
            os.remove(filename)
        except FileNotFoundError:
            logger.error('Could not delete temp file: %s' % filename)

    # Raise exception on error, if requested
    if check and p.returncode:
        raise CommandFailed(p.returncode, p.args, output=output, stderr=output)

    return subprocess.CompletedProcess(p.args, p.returncode, stdout=output, stderr=output)
