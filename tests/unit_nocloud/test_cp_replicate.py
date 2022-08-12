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


def test_map_objects_no_dir():
    assert map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar") == "bar"
    check_exception_raised(lambda: map_object_key_prefix("foo/a.txt", "foo/b.txt", "bar"), exceptions.MissingObjectException)
    assert map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar/") == "bar/a.txt"
    check_exception_raised(lambda: map_object_key_prefix("foo/", "bar", "foo/a.txt", "bar"), exceptions.MissingObjectException)
    check_exception_raised(lambda: map_object_key_prefix("foo", "foo/a.txt", "bar"), exceptions.MissingObjectException)


def test_map_objects_no_prefix_non_recursive():
    # destination no prefix
    assert map_object_key_prefix("a.txt", "a.txt", "") == "a.txt"
    assert map_object_key_prefix("a.txt", "a.txt", "/") == "a.txt"
    assert map_object_key_prefix("foo/a.txt", "foo/a.txt", "") == "a.txt"
    assert map_object_key_prefix("foo/a.txt", "foo/a.txt", "/") == "a.txt"
    check_exception_raised(lambda: map_object_key_prefix("foo/a.txt", "foo/b.txt", ""), exceptions.MissingObjectException)

    # source no prefix
    check_exception_raised(lambda: map_object_key_prefix("", "foo/b.txt", ""), exceptions.MissingObjectException)
    check_exception_raised(lambda: map_object_key_prefix("/", "foo/b.txt", ""), exceptions.MissingObjectException)
    check_exception_raised(lambda: map_object_key_prefix("", "foo/b.txt", "bar"), exceptions.MissingObjectException)


def test_map_objects_no_prefix_recursive():
    assert map_object_key_prefix("", "foo/bar/baz.txt", "", recursive=True) == "foo/bar/baz.txt"
    assert map_object_key_prefix("/", "foo/bar/baz.txt", "", recursive=True) == "foo/bar/baz.txt"
    assert map_object_key_prefix("/", "foo/bar/baz.txt", "/", recursive=True) == "foo/bar/baz.txt"
    assert map_object_key_prefix("", "foo/bar/baz.txt", "qux", recursive=True) == "qux/foo/bar/baz.txt"
    assert map_object_key_prefix("/", "foo/bar/baz.txt", "qux", recursive=True) == "qux/foo/bar/baz.txt"
    assert map_object_key_prefix("", "foo/bar/baz.txt", "qux/", recursive=True) == "qux/foo/bar/baz.txt"
    assert map_object_key_prefix("/", "foo/bar/baz.txt", "qux/", recursive=True) == "qux/foo/bar/baz.txt"
    assert map_object_key_prefix("/", "foo/bar/baz.txt", "qux", recursive=True) == "qux/foo/bar/baz.txt"
    assert map_object_key_prefix("foo", "foo/bar/baz.txt", "qux", recursive=True) == "qux/bar/baz.txt"
    assert map_object_key_prefix("foo/", "foo/bar/baz.txt", "qux", recursive=True) == "qux/bar/baz.txt"
    check_exception_raised(lambda: map_object_key_prefix("foo", "foobar/baz.txt", "", recursive=True), exceptions.MissingObjectException)


def test_map_objects_console_folder():
    assert map_object_key_prefix("foo", "foo", "qux") == "qux"
    assert map_object_key_prefix("foo", "foo", "qux/") == "qux/foo"
    assert map_object_key_prefix("foo/", "foo/", "qux/") == "qux/foo/"
    assert map_object_key_prefix("foo/", "foo/", "") == "foo/"
    assert map_object_key_prefix("foo/", "foo/", "/") == "foo/"
    assert map_object_key_prefix("foo/bar", "foo/bar", "qux/") == "qux/bar"

    assert map_object_key_prefix("", "foo/", "", recursive=True) == "foo/"
    assert map_object_key_prefix("/", "foo/", "", recursive=True) == "foo/"
    assert map_object_key_prefix("/", "foo/", "qux/", recursive=True) == "qux/foo/"


if __name__ == "__main__":
    test_map_object_single_file()
    test_map_object_recursive()
    test_map_objects_no_dir()
    test_map_objects_no_prefix_non_recursive()
