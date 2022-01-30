from datetime import datetime
import sys
from functools import partial

import termcolor


def log(msg, LEVEL="INFO", color="white", *args, **kwargs):
    if args or kwargs:
        msg = msg.format(*args, **kwargs)
    level_prefix = ("[" + LEVEL.upper() + "]").ljust(7)
    time = datetime.now().strftime("%H:%M:%S")
    print(f"{time} {level_prefix} {termcolor.colored(msg, color)}", flush=True, file=sys.stderr)


debug = partial(log, LEVEL="DEBUG", color="cyan")
info = partial(log, LEVEL="INFO", color="white")
warn = partial(log, LEVEL="WARN", color="yellow")
warning = partial(log, LEVEL="WARN", color="yellow")
error = partial(log, LEVEL="ERROR", color="red")


def exception(msg, *args, **kwargs):
    error(f"Exception: {msg}", *args, **kwargs)
    import traceback

    traceback.print_exc()
