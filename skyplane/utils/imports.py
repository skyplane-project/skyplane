import functools
import importlib


def inject(*modules, pip_extra=None):
    """
    Decorator for dependency injection
    @inject("google.auth")
    def example_fn(auth):
        # auth is now available
        auth.discovery.build()
    """

    def wrapper(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            modules_imported = []
            for module in modules:
                try:
                    modules_imported.append(importlib.import_module(module))
                except ImportError:
                    msg = f"Cannot import {module}."
                    if pip_extra:
                        msg += f" Install skyplane with {pip_extra} support: `pip install skyplane[{pip_extra.lower()}]`"
                    raise ImportError(msg)
            return fn(*modules_imported, *args, **kwargs)

        return wrapped

    return wrapper
