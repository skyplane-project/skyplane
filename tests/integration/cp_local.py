import argparse
import time
import os
import tempfile
import uuid
from skyplane.utils.definitions import MB
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.cli.cli import cp, sync
from skyplane.utils import logger


def setup_buckets(region, n_files=1, file_size_mb=1, write=False):
    provider, zone = region.split(":")
    if provider == "azure":
        bucket_name = f"integration{zone}/{str(uuid.uuid4()).replace('-', '')}"
    else:
        bucket_name = f"integration{zone}-{str(uuid.uuid4())[:8]}"
    logger.debug(f"creating buckets {bucket_name}")
    iface = ObjectStoreInterface.create(region, bucket_name)
    iface.create_bucket(zone)

    prefix = f"{uuid.uuid4()}"
    if write:
        with tempfile.NamedTemporaryFile() as tmp:
            fpath = tmp.name
            with open(fpath, "wb+") as f:
                f.write(os.urandom(int(file_size_mb * MB)))
            for i in range(n_files):
                iface.upload_object(fpath, f"{prefix}/{i}", mime_type="text/plain")

    return iface, bucket_name, prefix


def run(src_region, dest_region, n_files=1, file_size_mb=1, multipart=False):
    logger.info(
        f"Running skyplane [cp/sync] integration test with config "
        + f"src_region={src_region}, "
        + f"dest_region={dest_region}, "
        + f"n_files={n_files}, "
        + f"file_size_mb={file_size_mb}, "
        + f"multipart={multipart}"
    )

    # map region to path
    def map_path(region, bucket, prefix):
        provider, _ = region.split(":")
        if provider == "aws":
            return f"s3://{bucket}/{prefix}"
        elif provider == "azure":
            storage_account, container = bucket.split("/")
            return f"https://{storage_account}.blob.core.windows.net/{container}/{prefix}"
        elif provider == "gcp":
            return f"gs://{bucket}/{prefix}"
        else:
            raise Exception(f"Unknown provider {provider}")

    # create temporary files
    for mode in ["sync", "cp"]:
        return_code = 0
        if n_files == 1 and mode == "cp":
            tmp = tempfile.NamedTemporaryFile()
            fpath = tmp.name
            with open(fpath, "wb+") as f:
                f.write(os.urandom(int(file_size_mb * MB)))
        elif n_files > 1:  # create directory
            tmp = tempfile.TemporaryDirectory()
            fpath = tmp.name
            for i in range(n_files):
                with open(f"{fpath}/{i}", "wb+") as f:
                    f.write(os.urandom(int(file_size_mb * MB)))
        else:
            continue

        if src_region == "local":
            iface, bucket_name, prefix = setup_buckets(dest_region)
            if mode == "cp":
                return_code = cp(
                    fpath,
                    map_path(dest_region, bucket_name, prefix),
                    recursive=(n_files > 1),
                    debug=False,
                    multipart=multipart,
                    confirm=True,
                    max_instances=1,
                    max_connections=1,
                    solver="direct",
                    solver_required_throughput_gbits=1,
                )
            elif n_files > 1:
                return_code = sync(
                    fpath,
                    map_path(dest_region, bucket_name, prefix),
                    debug=False,
                    multipart=multipart,
                    confirm=True,
                    max_instances=1,
                    max_connections=1,
                    solver="direct",
                    solver_required_throughput_gbits=1,
                )
        elif dest_region == "local":
            iface, bucket_name, prefix = setup_buckets(src_region, n_files=n_files, file_size_mb=file_size_mb, write=True)
            if mode == "cp":
                return_code = cp(
                    map_path(src_region, bucket_name, prefix),
                    fpath,
                    recursive=True,
                    debug=False,
                    multipart=multipart,
                    confirm=True,
                    max_instances=1,
                    max_connections=1,
                    solver="direct",
                    solver_required_throughput_gbits=1,
                )
            elif n_files > 1:
                return_code = sync(
                    map_path(src_region, bucket_name, prefix),
                    fpath,
                    debug=False,
                    multipart=multipart,
                    confirm=True,
                    max_instances=1,
                    max_connections=1,
                    solver="direct",
                    solver_required_throughput_gbits=1,
                )
        else:
            raise ValueError("This script only tests local transfers")

        iface.delete_bucket()

        if return_code > 0:
            return return_code

    return return_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("src", help="source region")
    parser.add_argument("dest", help="destination region")
    parser.add_argument("--n-files", type=int, default=1)
    parser.add_argument("--file-size-mb", type=int, default=1)
    parser.add_argument("--multipart", action="store_true")
    args = parser.parse_args()

    return_code = run(args.src, args.dest, n_files=args.n_files, file_size_mb=args.file_size_mb, multipart=args.multipart)
    exit(return_code)
