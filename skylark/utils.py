import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm


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


def do_parallel(func, args_list, n=6, progress_bar=False):
    """Run list of jobs in parallel with tqdm progress bar"""
    results = []
    pbar = tqdm(total=len(args_list), leave=False) if progress_bar else None
    with ThreadPoolExecutor(max_workers=n) as executor:
        future_list = [executor.submit(func, args) for args in args_list]
        for args, future in zip(args_list, as_completed(future_list)):
            if pbar:
                pbar.set_description(str(args))
                pbar.update()
            results.append(future.result())
    if pbar:
        pbar.close()
    return results


def common_excludes(
        ignore_dirs=[
            ".git",
            "__pycache__",
            ".ipynb_checkpoints",
            "nb",
            "data",
            "*.egg-info",
        ],
        ignore_recursive_dirs=["__pycache__"],
        ignore_exts=["pyc", "pyo", "swp"],
):
    ignore_full_path = (
            [str(p) for p in ignore_dirs]
            + [f"{p}/**" for p in ignore_dirs]
            + [f"{p}/**/*" for p in ignore_dirs]
    )
    ignore_full_path += [f"**/{p}" for p in ignore_recursive_dirs] + [
        f"**/{p}/*" for p in ignore_recursive_dirs
    ]
    ignore_exts = [f"*.{e}" for e in ignore_exts]
    return ignore_full_path + ignore_exts