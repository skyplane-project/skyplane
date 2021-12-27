import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Union

from tqdm import tqdm

PathLike = Union[str, Path]


class Timer:
    def __init__(self):
        self.start = time.time()
        self.end = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.time()

    @property
    def elapsed(self):
        return self.end - self.start


def do_parallel(func, args_list, n=8, progress_bar=False, leave_pbar=True, desc=None, arg_fmt=None):
    """Run list of jobs in parallel with tqdm progress bar"""
    if arg_fmt is None:
        arg_fmt = lambda x: x.region_tag if hasattr(x, "region_tag") else x

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
