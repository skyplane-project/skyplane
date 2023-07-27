from skyplane import exceptions
from skyplane.api.transfer_job import Chunker


def check_exception_raised(func, exception_type, exception_msg=None):
    try:
        func()
        assert False
    except exception_type as e:
        if exception_msg is not None:
            assert exception_msg in str(e)


def test_map_object_single_file():
    assert Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar") == "bar"
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo/a.txt", "foo/b.txt", "bar"), exceptions.MissingObjectException)
    assert Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar/") == "bar/a.txt"
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo/", "bar", "foo/a.txt", "bar"), exceptions.MissingObjectException)
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo", "foo/a.txt", "bar"), exceptions.MissingObjectException)


def test_map_object_recursive():
    assert Chunker.map_object_key_prefix("foo/", "foo/a.txt", "bar", recursive=True) == "bar/a.txt"
    assert Chunker.map_object_key_prefix("foo/", "foo/b.txt", "bar", recursive=True) == "bar/b.txt"
    assert Chunker.map_object_key_prefix("foo", "foo/c.txt", "bar", recursive=True) == "bar/c.txt"
    check_exception_raised(
        lambda: Chunker.map_object_key_prefix("foo", "fooc.txt", "bar", recursive=True), exceptions.MissingObjectException
    )
    check_exception_raised(
        lambda: Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar", recursive=True), exceptions.MissingObjectException
    )
    check_exception_raised(
        lambda: Chunker.map_object_key_prefix("foo/a.txt", "foo/b.txt", "bar/", recursive=True), exceptions.MissingObjectException
    )
    assert Chunker.map_object_key_prefix("bar/foo/", "bar/foo/b.txt", "bar", recursive=True) == "bar/b.txt"


def test_map_objects_no_dir():
    assert Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar") == "bar"
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo/a.txt", "foo/b.txt", "bar"), exceptions.MissingObjectException)
    assert Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "bar/") == "bar/a.txt"
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo/", "bar", "foo/a.txt", "bar"), exceptions.MissingObjectException)
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo", "foo/a.txt", "bar"), exceptions.MissingObjectException)


def test_map_objects_no_prefix_non_recursive():
    # destination no prefix
    assert Chunker.map_object_key_prefix("a.txt", "a.txt", "") == "a.txt"
    assert Chunker.map_object_key_prefix("a.txt", "a.txt", "/") == "a.txt"
    assert Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "") == "a.txt"
    assert Chunker.map_object_key_prefix("foo/a.txt", "foo/a.txt", "/") == "a.txt"
    check_exception_raised(lambda: Chunker.map_object_key_prefix("foo/a.txt", "foo/b.txt", ""), exceptions.MissingObjectException)

    # source no prefix
    check_exception_raised(lambda: Chunker.map_object_key_prefix("", "foo/b.txt", ""), exceptions.MissingObjectException)
    check_exception_raised(lambda: Chunker.map_object_key_prefix("/", "foo/b.txt", ""), exceptions.MissingObjectException)
    check_exception_raised(lambda: Chunker.map_object_key_prefix("", "foo/b.txt", "bar"), exceptions.MissingObjectException)


def test_map_objects_no_prefix_recursive():
    assert Chunker.map_object_key_prefix("", "foo/bar/baz.txt", "", recursive=True) == "foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("/", "foo/bar/baz.txt", "", recursive=True) == "foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("/", "foo/bar/baz.txt", "/", recursive=True) == "foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("", "foo/bar/baz.txt", "qux", recursive=True) == "qux/foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("/", "foo/bar/baz.txt", "qux", recursive=True) == "qux/foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("", "foo/bar/baz.txt", "qux/", recursive=True) == "qux/foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("/", "foo/bar/baz.txt", "qux/", recursive=True) == "qux/foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("/", "foo/bar/baz.txt", "qux", recursive=True) == "qux/foo/bar/baz.txt"
    assert Chunker.map_object_key_prefix("foo", "foo/bar/baz.txt", "qux", recursive=True) == "qux/bar/baz.txt"
    assert Chunker.map_object_key_prefix("foo/", "foo/bar/baz.txt", "qux", recursive=True) == "qux/bar/baz.txt"
    check_exception_raised(
        lambda: Chunker.map_object_key_prefix("foo", "foobar/baz.txt", "", recursive=True), exceptions.MissingObjectException
    )


def test_map_objects_console_folder():
    assert Chunker.map_object_key_prefix("foo", "foo", "qux") == "qux"
    assert Chunker.map_object_key_prefix("foo", "foo", "qux/") == "qux/foo"
    assert Chunker.map_object_key_prefix("foo/", "foo/", "qux/") == "qux/foo/"
    assert Chunker.map_object_key_prefix("foo/", "foo/", "") == "foo/"
    assert Chunker.map_object_key_prefix("foo/", "foo/", "/") == "foo/"
    assert Chunker.map_object_key_prefix("foo/bar", "foo/bar", "qux/") == "qux/bar"

    assert Chunker.map_object_key_prefix("", "foo/", "", recursive=True) == "foo/"
    assert Chunker.map_object_key_prefix("/", "foo/", "", recursive=True) == "foo/"
    assert Chunker.map_object_key_prefix("/", "foo/", "qux/", recursive=True) == "qux/foo/"


def test_issue_490():  # from issue #490
    assert Chunker.map_object_key_prefix("/", "foo/", "/", recursive=True) == "foo/"
    assert Chunker.map_object_key_prefix("/", "foo", "/", recursive=True) == "foo"
    assert Chunker.map_object_key_prefix("", "foo", "/", recursive=True) == "foo"
    assert Chunker.map_object_key_prefix("/", "foo/", "", recursive=True) == "foo/"
    assert Chunker.map_object_key_prefix("", "foo/", "", recursive=True) == "foo/"
