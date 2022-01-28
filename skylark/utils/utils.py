import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Union

from skylark.utils import logger
from tqdm import tqdm

PathLike = Union[str, Path]


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
            logger.debug(f"{self.print_desc}: {self.end - self.start:.2f}s")

    @property
    def elapsed(self):
        if self.end is None:
            return time.time() - self.start
        else:
            return self.end - self.start


def wait_for(fn, timeout=60, interval=0.25, progress_bar=False, desc="Waiting", leave_pbar=True):
    # wait for fn to return True
    start = time.time()
    with tqdm(desc=desc, leave=leave_pbar, disable=not progress_bar) as pbar:
        while time.time() - start < timeout:
            if fn():
                pbar.close()
                return True
            pbar.update(interval)
            time.sleep(interval)
        raise Exception(f"Timeout waiting for {desc}")


def do_parallel(func, args_list, n=-1, progress_bar=False, leave_pbar=True, desc=None, arg_fmt=None):
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
                pbar.set_description(f"{desc} ({str(arg_fmt(args))})" if desc else str(arg_fmt(args)))
                pbar.update()
        return results
