import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Iterable, List, Tuple, Union, TypeVar


from skyplane.utils import logger
from skyplane.utils.timer import Timer

PathLike = Union[str, Path]
T = TypeVar("T")
R = TypeVar("R")


def wait_for(fn: Callable[[], bool], timeout=60, interval=0.25, spinner=False, desc="Waiting", leave_spinner=True) -> bool:
    if spinner:  # check to avoid importing on gateway
        from halo import Halo

    # wait for fn to return True
    start = time.time()
    with Halo({desc}, spinner="dots", enabled=spinner) as spinner_obj:
        while time.time() - start < timeout:
            if fn():
                logger.fs.debug(f"[wait_for] {desc} fn={fn} completed in {time.time() - start:.2f}s")
                if leave_spinner:
                    spinner_obj.succeed(f"[wait_for] {desc} fn={fn} completed in {time.time() - start:.2f}s")
                return True
            time.sleep(interval)
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
    if spinner:  # check to avoid importing on gateway
        from halo import Halo

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
    if spinner:
        spinner_obj = Halo(f"{desc} ({len(results)}/{len(args_list)})", spinner="dots")
        spinner_obj.start()
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
    if spinner and spinner_persist:
        spinner_obj.succeed(f"{desc} ({len(output)}/{len(args_list)}) in {t.elapsed:.2f}s")
    elif spinner:
        spinner_obj.stop()
    return output


def retry_backoff(
    fn: Callable[[], R],
    max_retries=8,
    initial_backoff=0.1,
    max_backoff=8,
    exception_class=Exception,
    log_errors=True,
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
                # ignore retries due to IAM instance profile propagation
                if log_errors:
                    fn_name = fn.__name__ if hasattr(fn, "__name__") else "unknown function"
                    logger.warning(f"Retrying {fn_name} due to: {e} (attempt {i + 1}/{max_retries})")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
