import argparse
import os
import tempfile
import uuid
from skyplane.utils.definitions import KB
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.cli.cli import sync
from skyplane.utils import logger


def setup_buckets(src_region, dest_region, n_files=1, file_size_kb=1):
    src_provider, src_zone = src_region.split(":")
    dest_provider, dest_zone = dest_region.split(":")
    if src_provider == "azure":
        src_bucket_name = f"integration{src_zone}/{str(uuid.uuid4()).replace('-', '')}"
    else:
        src_bucket_name = f"integration{src_zone}-{str(uuid.uuid4())[:8]}"
    if dest_provider == "azure":
        dest_bucket_name = f"integration{dest_zone}/{str(uuid.uuid4()).replace('-', '')}"
    else:
        dest_bucket_name = f"skyplane-integration-{dest_zone}-{str(uuid.uuid4())[:8]}"
    logger.debug(f"creating buckets {src_bucket_name} and {dest_bucket_name}")
    src_interface = ObjectStoreInterface.create(src_region, src_bucket_name)
    dest_interface = ObjectStoreInterface.create(dest_region, dest_bucket_name)
    src_interface.create_bucket(src_zone)
    dest_interface.create_bucket(dest_zone)

    src_prefix = f"src_{uuid.uuid4()}"
    dest_prefix = f"dest_{uuid.uuid4()}"
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "wb+") as f:
            f.write(os.urandom(int(file_size_kb * KB)))
        for i in range(n_files):
            src_interface.upload_object(fpath, f"{src_prefix}/{i}", mime_type="text/plain")

    return src_bucket_name, dest_bucket_name, src_prefix, dest_prefix


def run(src_region, dest_region, n_files=1, file_size_kb=1, multipart=False):
    logger.info(
        f"Running skyplane sync integration test with config "
        + f"src_region={src_region}, "
        + f"dest_region={dest_region}, "
        + f"n_files={n_files}, "
        + f"file_size_kb={file_size_kb}, "
        + f"multipart={multipart}"
    )
    src_bucket_name, dest_bucket_name, src_prefix, dest_prefix = setup_buckets(
        src_region, dest_region, n_files=n_files, file_size_kb=file_size_kb
    )

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

    return_code = sync(
        map_path(src_region, src_bucket_name, src_prefix),
        map_path(dest_region, dest_bucket_name, dest_prefix),
        debug=False,
        multipart=multipart,
        confirm=True,
        max_instances=1,
        max_connections=1,
        solver="direct",
        solver_required_throughput_gbits=1,
    )

    # clean up path
    src_interface = ObjectStoreInterface.create(src_region, src_bucket_name)
    dest_interface = ObjectStoreInterface.create(dest_region, dest_bucket_name)
    src_interface.delete_objects([f"{src_prefix}/{i}" for i in range(n_files)])
    dest_interface.delete_objects([f"{dest_prefix}/{i}" for i in range(n_files)])
    src_interface.delete_bucket()
    dest_interface.delete_bucket()

    return return_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("src", help="source region")
    parser.add_argument("dest", help="destination region")
    parser.add_argument("--n-files", type=int, default=1)
    parser.add_argument("--file-size-kb", type=int, default=1)
    parser.add_argument("--multipart", action="store_true")
    args = parser.parse_args()

    return_code = run(args.src, args.dest, n_files=args.n_files, file_size_kb=args.file_size_kb, multipart=args.multipart)
    exit(return_code)
