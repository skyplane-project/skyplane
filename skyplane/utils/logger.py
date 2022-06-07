import sys
from datetime import datetime
from functools import partial
from types import SimpleNamespace

from rich import print as rprint

from skyplane import is_gateway_env

log_file = None


def open_log_file(filename):
    global log_file
    log_file = open(filename, "a")


def log(msg, LEVEL="INFO", color="white", write_to_file=True, write_to_stderr=True, *args, **kwargs):
    if args or kwargs:
        msg = msg.format(*args, **kwargs)
    level_prefix = ("[" + LEVEL.upper() + "]").ljust(7)
    time = datetime.now().strftime("%H:%M:%S")
    if write_to_file and log_file:
        log_file.write(f"{time} {level_prefix} {msg}\n")
        log_file.flush()
    if write_to_stderr:
        rprint(f"{time} {level_prefix} [{color}]{msg}[/]", flush=True, file=sys.stderr)


debug = partial(log, LEVEL="DEBUG", color="cyan")
info = partial(log, LEVEL="INFO", color="white")
warn = partial(log, LEVEL="WARN", color="yellow")
warning = partial(log, LEVEL="WARN", color="yellow")
error = partial(log, LEVEL="ERROR", color="red")


def exception(msg, print_traceback=True, write_to_file=False, *args, **kwargs):
    error(f"Exception: {msg}", write_to_file=write_to_file, *args, **kwargs)
    if print_traceback:
        import traceback

        if write_to_file:
            traceback.print_exc(file=log_file)
        else:
            traceback.print_exc()


# define fs object to log to disk log
fs = SimpleNamespace(
    debug=partial(log, LEVEL="DEBUG", color="cyan", write_to_file=True, write_to_stderr=is_gateway_env),
    info=partial(log, LEVEL="INFO", color="white", write_to_file=True, write_to_stderr=is_gateway_env),
    warn=partial(log, LEVEL="WARN", color="yellow", write_to_file=True, write_to_stderr=is_gateway_env),
    warning=partial(log, LEVEL="WARN", color="yellow", write_to_file=True, write_to_stderr=is_gateway_env),
    error=partial(log, LEVEL="ERROR", color="red", write_to_file=True, write_to_stderr=is_gateway_env),
    exception=partial(exception, write_to_file=True, write_to_stderr=is_gateway_env),
    log=partial(log, write_to_file=True, write_to_stderr=is_gateway_env),
)
