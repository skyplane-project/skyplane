import time

from skyplane.utils import logger


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
            logger.fs.debug(f"{self.print_desc}: {self.elapsed:.2f}s")

    @property
    def elapsed(self):
        if self.end is None:
            end = time.time()
            return end - self.start
        else:
            return self.end - self.start
