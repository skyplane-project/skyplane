import time
from typing import Callable, TypeVar

from skyplane.utils import logger


R = TypeVar("R")


def retry_backoff(
    fn: Callable[[], R],
    max_retries=8,
    initial_backoff=0.1,
    max_backoff=8,
    exception_class=Exception,
    log_errors=True,
    always_raise_exceptions=(),
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
            if i == max_retries - 1 or type(e) in always_raise_exceptions:
                raise e
            else:
                # ignore retries due to IAM instance profile propagation
                if log_errors:
                    fn_name = fn.__name__ if hasattr(fn, "__name__") else "unknown function"
                    logger.fs.warning(f"Retrying {fn_name} due to: {e} (attempt {i + 1}/{max_retries})")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
