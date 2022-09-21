import functools
import importlib


def inject(*modules, pip_extra=None):
    """
    Decorator for dependency injection
    @inject("google.auth")
    def example_fn(auth):
        auth.discovery.build()
    """

    def wrapper(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            modules_imported = []
            err_msg = f"Cannot import {module}."
            if pip_extra:
                err_msg += f" Install skyplane with {pip_extra} support: `pip install skyplane[{pip_extra.lower()}]`"
            for module in modules:
                try:
                    modules_imported.append(importlib.import_module(module))
                except ImportError as e:
                    # try the "from x import y" syntax
                    module_split = module.split(".")
                    if len(module_split) > 1:
                        try:
                            module = importlib.import_module(".".join(module_split[:-1]))
                            modules_imported.append(getattr(module, module_split[-1]))
                        except (ImportError, AttributeError):
                            raise ImportError(err_msg) from e
                    else:
                        raise ImportError(err_msg) from e
            return fn(*modules_imported, *args, **kwargs)

        return wrapped

    return wrapper
