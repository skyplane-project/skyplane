from skyplane import exceptions
from skyplane.cli.cli_impl.cp_replicate import map_object_key_prefix


def check_exception_raised(func, exception_type, exception_msg=None):
    try:
        func()
        assert False
    except exception_type as e:
        if exception_msg is not None:
            assert exception_msg in str(e)


def test_map_object_single_file():
    assert map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar") == "bar"
    check_exception_raised(lambda: map_object_key_prefix("foo/a.txt", "foo/b.txt", "bar"), exceptions.MissingObjectException)
    assert map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar/") == "bar/a.txt"
    check_exception_raised(lambda: map_object_key_prefix("foo/", "bar", "foo/a.txt", "bar"), exceptions.MissingObjectException)
    check_exception_raised(lambda: map_object_key_prefix("foo", "foo/a.txt", "bar"), exceptions.MissingObjectException)


def test_map_object_recursive():
    assert map_object_key_prefix("foo/", "foo/a.txt", "bar", recursive=True) == "bar/a.txt"
    assert map_object_key_prefix("foo/", "foo/b.txt", "bar", recursive=True) == "bar/b.txt"
    assert map_object_key_prefix("foo", "foo/c.txt", "bar", recursive=True) == "bar/c.txt"
    check_exception_raised(lambda: map_object_key_prefix("foo", "fooc.txt", "bar", recursive=True), exceptions.MissingObjectException)
    check_exception_raised(
        lambda: map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar", recursive=True), exceptions.MissingObjectException
    )
    check_exception_raised(
        lambda: map_object_key_prefix("foo/a.txt", "foo/b.txt", "bar/", recursive=True), exceptions.MissingObjectException
    )
    assert map_object_key_prefix("bar/foo/", "bar/foo/b.txt", "bar", recursive=True) == "bar/b.txt"
