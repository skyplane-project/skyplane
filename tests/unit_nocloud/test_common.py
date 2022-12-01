from skyplane.utils.path import parse_path


def test_parse_path():
    # test local
    assert parse_path("/") == ("local", None, "/")
    assert parse_path("/tmp") == ("local", None, "/tmp")
    assert parse_path("does-not-exist-0000000/file") == ("local", None, "does-not-exist-0000000/file")

    # test s3://
    assert parse_path("s3://bucket") == ("aws", "bucket", "")
    assert parse_path("s3://bucket/") == ("aws", "bucket", "")
    assert parse_path("s3://bucket/key") == ("aws", "bucket", "key")

    # test gs://
    assert parse_path("gs://bucket") == ("gcp", "bucket", "")
    assert parse_path("gs://bucket/") == ("gcp", "bucket", "")
    assert parse_path("gs://bucket/key") == ("gcp", "bucket", "key")

    # test https:// azure
    assert parse_path("https://bucket.blob.core.windows.net/container") == ("azure", "bucket/container", "")
    assert parse_path("https://bucket.blob.core.windows.net/container/") == ("azure", "bucket/container", "")
    assert parse_path("https://bucket.blob.core.windows.net/container/key") == ("azure", "bucket/container", "key")

    # test azure:// azure
    assert parse_path("azure://bucket/container") == ("azure", "bucket/container", "")
    assert parse_path("azure://bucket/container/") == ("azure", "bucket/container", "")
    assert parse_path("azure://bucket/container/key") == ("azure", "bucket/container", "key")
    assert parse_path("azure://bucket/container/key/path") == ("azure", "bucket/container", "key/path")
