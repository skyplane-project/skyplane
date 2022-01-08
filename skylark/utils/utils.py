import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Union

from loguru import logger
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
        return self.end - self.start


def wait_for(fn, timeout=60, interval=1, progress_bar=False, desc="Waiting", leave_pbar=True):
    # wait for fn to return True
    start = time.time()
    if progress_bar:
        pbar = tqdm(desc=desc, leave=leave_pbar)
    while time.time() - start < timeout:
        if fn():
            if progress_bar:
                pbar.close()
                print()
            return True
        if progress_bar:
            pbar.update(interval)
        time.sleep(interval)
    if progress_bar:
        pbar.close()
    raise Exception("Timeout")


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
    pbar = tqdm(total=len(args_list), leave=leave_pbar, desc=desc) if progress_bar else None
    with ThreadPoolExecutor(max_workers=n) as executor:
        future_list = [executor.submit(wrapped_fn, args) for args in args_list]
        for future in as_completed(future_list):
            args, result = future.result()
            results.append((args, result))
            if pbar:
                pbar.set_description(f"{desc} ({str(arg_fmt(args))})" if desc else str(arg_fmt(args)))
                pbar.update()
    if pbar:
        pbar.close()
    return results
