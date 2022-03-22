import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Iterable, List, Tuple, Union, TypeVar

from skylark.utils import logger
from tqdm import tqdm

PathLike = Union[str, Path]
T = TypeVar("T")
R = TypeVar("R")


class Timer:
    def __init__(self, print_desc=None):
        self.print_desc = print_desc
        self.start = time.time()
        self.end = None

    def __enter__(self):
        return self

    def __exit__(self, exc_typ, exc_val, exc_tb):
        self.end = time.time()
        if self.print_desc:
            logger.debug(f"{self.print_desc}: {self.elapsed:.2f}s")

    @property
    def elapsed(self):
        if self.end is None:
            end = time.time()
            return end - self.start
        else:
            return self.end - self.start


def wait_for(fn: Callable[[], bool], timeout=60, interval=0.25, progress_bar=False, desc="Waiting", leave_pbar=True) -> bool:
    # wait for fn to return True
    start = time.time()
    with tqdm(desc=desc, leave=leave_pbar, disable=not progress_bar) as pbar:
        while time.time() - start < timeout:
            if fn():
                pbar.close()
                return True
            pbar.update(interval)
            time.sleep(interval)
        raise TimeoutError(f"Timeout waiting for {desc}")


def do_parallel(
    func: Callable[[T], R], args_list: Iterable[T], n=-1, progress_bar=False, leave_pbar=True, desc=None, arg_fmt=None, hide_args=False
) -> List[Tuple[T, R]]:
    """Run list of jobs in parallel with tqdm progress bar"""
    args_list = list(args_list)
    if len(args_list) == 0:
        return []

    if arg_fmt is None:
        arg_fmt = lambda x: x.region_tag if hasattr(x, "region_tag") else x

    if n == -1:
        n = len(args_list)

    def wrapped_fn(args):
        return args, func(args)

    results = []
    with tqdm(total=len(args_list), leave=leave_pbar, desc=desc, disable=not progress_bar) as pbar:
        with ThreadPoolExecutor(max_workers=n) as executor:
            future_list = [executor.submit(wrapped_fn, args) for args in args_list]
            for future in as_completed(future_list):
                args, result = future.result()
                results.append((args, result))
                if not hide_args:
                    pbar.set_description(f"{desc} ({str(arg_fmt(args))})" if desc else str(arg_fmt(args)))
                else:
                    pbar.set_description(desc)
                pbar.update()
        return results


def retry_backoff(
    fn: Callable[[], R],
    max_retries=8,
    initial_backoff=0.1,
    max_backoff=8,
    exception_class=Exception,
) -> R:
    """Retry fn until it does not raise an exception.
    If it fails, sleep for a bit and try again.
    Double the backoff time each time.
    If it fails max_retries times, raise the last exception.
    """
    backoff = initial_backoff
    for i in range(max_retries):
        try:
            return fn()
        except exception_class as e:
            if i == max_retries - 1:
                raise e
            else:
                logger.warning(f"Retrying function due to {e} (attempt {i + 1}/{max_retries})")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
