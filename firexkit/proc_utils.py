import psutil
import re

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def kill_procs_by_name_and_cmdline(proc_name, cmdline_regex_str=None) -> list[psutil.Process]:
    if cmdline_regex_str:
        cmdline_regex = re.compile(cmdline_regex_str)
    else:
        cmdline_regex = None

    killed_procs = []
    for proc in psutil.process_iter():
        try:
            proc_info_dict = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
        except psutil.NoSuchProcess:
            pass
        except IndexError as e:
            # psutil can fail with an IndexError when attempting to inspect some username values, such as nginx
            # worker processes launched via docker/supodman.
            logger.warning(f"Failure trying to read process details: {e}")
        else:
            if (
                proc_name == proc_info_dict['name']
                and (
                    cmdline_regex is None
                    or any(
                        re.match(cmdline_regex, cmd_item)
                        for cmd_item in proc_info_dict['cmdline']
                    )
                )
            ):
                try:
                    logger.debug(f"Killing {proc_info_dict['pid']}")
                    proc.kill()
                except Exception as e:
                    logger.debug(f'------ FAILED {e}' % e)
                else:
                    killed_procs.append(proc)

    return killed_procs
