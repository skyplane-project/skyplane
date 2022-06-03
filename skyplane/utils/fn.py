import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Iterable, List, Tuple, Union, TypeVar

from halo import Halo

from skyplane.utils import logger
from skyplane.utils.timer import Timer

PathLike = Union[str, Path]
T = TypeVar("T")
R = TypeVar("R")


def wait_for(fn: Callable[[], bool], timeout=60, interval=0.25, spinner=False, desc="Waiting", leave_spinner=True) -> bool:
    """Wait for fn to return True"""
    with Halo({desc}, spinner="dots", enabled=spinner) as spinner_obj:
        start = time.time()
        while time.time() - start < timeout:
            if fn():
                logger.fs.debug(f"[wait_for] {desc} fn={fn} completed in {time.time() - start:.2f}s")
                if spinner and leave_spinner:
                    spinner_obj.succeed(f"[wait_for] {desc} fn={fn} completed in {time.time() - start:.2f}s")
                return True
            time.sleep(interval)
            if spinner:
                spinner_obj.fail(f"[wait_for] {desc} fn={fn} timed out after {time.time() - start:.2f}s")
            raise TimeoutError(f"Timeout waiting for {desc}")


def do_parallel(
    func: Callable[[T], R],
    args_list: Iterable[T],
    n=-1,
    desc=None,
    arg_fmt=None,
    return_args=True,
    spinner=False,
    spinner_persist=False,
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
            logger.error(f"Error running {func.__name__} with args {arg_fmt(args)}: {e}")
            raise e

    results = []
    with Halo(f"{desc} ({len(results)}/{len(args_list)})", spinner="dots", enabled=spinner) as spinner_obj:
        with Timer() as t:
            with ThreadPoolExecutor(max_workers=n) as executor:
                future_list = [executor.submit(wrapped_fn, args) for args in args_list]
                for future in as_completed(future_list):
                    args, result = future.result()
                    results.append((args, result))
                    if spinner:
                        spinner_obj.text = f"{desc} ({len(results)}/{len(args_list)})"
            if return_args:
                output = results
            else:
                output = [result for _, result in results]
        if spinner_persist:
            spinner_obj.succeed(f"{desc} ({len(output)}/{len(args_list)}) in {t.elapsed:.2f}s")
        return output
