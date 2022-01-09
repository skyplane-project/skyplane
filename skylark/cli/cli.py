"""
CLI for the Skylark object store.

Usage mostly matches the aws-cli command line tool:
`skylark [command] [subcommand] [flags] [args]`


* `skylark ls [directory]`: List objects in the object store.
* `skylark cp [flags] [args]`: Copy objects from the object store to the local filesystem.
* `skylark mv [flags] [args]`: Move objects from the object store to the local filesystem.
* `skylark rm [flags] [args]`: Remove objects from the object store.


Copying to/from local files:
* `skylark cp [flags] /path/to/file.txt s3://bucket/path/to/file.txt`
* `skylark cp [flags] s3://bucket/path/to/file.txt /path/to/file.txt`
* (not supported) `skylark cp [flags] s3://bucket/path/to/file.txt /path/to/file.txt`

Replicating between remote object stores:
* `skylark replicate [flags] s3://bucket/path/to/file.txt s3://bucket/path/to/file.txt`
"""


from pathlib import Path
import typer

from skylark.cli.cli_helper import copy_local_local, copy_local_s3, copy_s3_local, ls_local, ls_s3, parse_path

app = typer.Typer()


@app.command()
def ls(directory: str):
    """List objects in the object store."""
    provider, bucket, key = parse_path(directory)
    if provider == "local":
        for path in ls_local(Path(directory)):
            print(path)
    elif provider == "s3":
        for path in ls_s3(bucket, key):
            print(path)


@app.command()
def cp(src: str, dst: str):
    """Copy objects from the object store to the local filesystem."""
    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)

    if provider_src == "local" and provider_dst == "local":
        copy_local_local(Path(path_src), Path(path_dst))
    elif provider_src == "local" and provider_dst == "s3":
        copy_local_s3(Path(path_src), bucket_dst, path_dst)
    elif provider_src == "s3" and provider_dst == "local":
        copy_s3_local(bucket_src, path_src, Path(path_dst))
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


if __name__ == "__main__":
    app()
