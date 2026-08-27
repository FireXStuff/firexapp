import gc
import inspect
import tracemalloc
from contextlib import contextmanager
import psutil
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def get_process_memory_info(pid=None, gc_collect=True):
    if gc_collect:
        gc.enable()
        gc.collect()
    process = psutil.Process(pid)
    return process.memory_full_info()


def human_readable_bytes(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num}{unit}{suffix}" if unit == '' else f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def tracemalloc_compare(snapshot_initial, snapshot_final, top_differences=3):
    output = []
    top_stats = snapshot_final.compare_to(snapshot_initial, 'lineno')
    output += [f'[ Top {top_differences} differences ]']
    for stat in top_stats[:top_differences]:
        output += [f'{stat}']
    return output


@contextmanager
def process_memory_delta(prefix='', trace_mem=True):
    snapshot_initial = None
    output = []
    if prefix:
        prefix = f'[{prefix}]'
    frame1 = inspect.stack()[2]
    mem_initial = get_process_memory_info()
    if trace_mem:
        tracemalloc.start(20)
        snapshot_initial = tracemalloc.take_snapshot()
    try:
        yield
    finally:
        mem_final = get_process_memory_info()
        vms_delta = mem_final.vms - mem_initial.vms
        rss_delta = mem_final.rss - mem_initial.rss
        frame2 = inspect.stack()[2]
        output += [f'{prefix}[{frame1.function}:{frame1.lineno}->{frame2.function}:{frame2.lineno}]']
        output += [f'rss delta={human_readable_bytes(rss_delta)}, vms delta={human_readable_bytes(vms_delta)}']
        if trace_mem:
            snapshot_final = tracemalloc.take_snapshot()
            output += tracemalloc_compare(snapshot_initial, snapshot_final)
        logger.debug('\n'.join(output))
