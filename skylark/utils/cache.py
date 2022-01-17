import cachetools


class IngoreLRUCache(cachetools.LRUCache):
    """Extension of cache that does not store values"""

    def __init__(self, ignored_value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignored_value = ignored_value

    def __setitem__(self, key, value):
        if value != self.ignored_value:
            super().__setitem__(key, value)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        if value == self.ignored_value:
            raise KeyError(key)
        return value


def ignore_lru_cache(ignored_value=None, maxsize=128, *args, **kwargs):
    """Decorator to ignore LRU cache when value is ignored_value"""

    def decorator(func):
        return cachetools.cached(IngoreLRUCache(ignored_value, maxsize=maxsize, *args, **kwargs))(func)

    return decorator
