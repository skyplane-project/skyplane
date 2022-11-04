import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from typing import Callable, Iterable, List, Optional, Tuple, Union, TypeVar

from skyplane.utils import logger
from skyplane.utils.timer import Timer

PathLike = Union[str, Path]
T = TypeVar("T")
R = TypeVar("R")


def wait_for(fn: Callable[[], bool], timeout=60, interval=0.25, desc="Waiting", debug=False) -> Optional[float]:
    """Wait for fn to return True. Returns number of seconds waited."""
    start = time.time()
    while time.time() - start < timeout:
        if fn() == True:
            logger.fs.debug(f"[wait_for] {desc} fn={fn} completed in {time.time() - start:.2f}s")
            return time.time() - start
        time.sleep(interval)
    raise TimeoutError(f"Timeout waiting for '{desc}' (timeout {timeout:.2f}s, interval {interval:.2f}s)")


def do_parallel(
    func: Callable[[T], R], args_list: Iterable[T], n=-1, desc=None, arg_fmt=None, return_args=True, spinner=False, spinner_persist=False
) -> List[Union[Tuple[T, R], R]]:
    args_list = list(args_list)
    if len(args_list) == 0:
        return []

    if arg_fmt is None:
        arg_fmt = lambda x: x.region_tag if hasattr(x, "region_tag") else x

    if n == -1:
        n = len(args_list)

    def wrapped_fn(args):
        try:
            return args, func(args)
        except Exception as e:
            logger.error(f"Error running {func.__name__}: {e}")
            raise

    results = []
    with Progress(
        SpinnerColumn(), TextColumn(desc), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn(), disable=not spinner, transient=True
    ) as progress:
        progress_task = progress.add_task("", total=len(args_list))
        with Timer() as t:
            with ThreadPoolExecutor(max_workers=n) as executor:
                future_list = [executor.submit(wrapped_fn, args) for args in args_list]
                for future in as_completed(future_list):
                    args, result = future.result()
                    results.append((args, result))
                    progress.update(progress_task, advance=1)
    if spinner_persist:
        rprint(f"[bold green]✓[/] [bright_black]{desc} ({len(results)}/{len(args_list)}) in {t.elapsed:.2f}s[/]")
    return results if return_args else [result for _, result in results]
