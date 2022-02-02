from skylark.obj_store.object_store_interface import ObjectStoreInterface
from skylark.utils.utils import do_parallel
from skylark.utils.utils import PathLike, Timer, wait_for
from tqdm import tqdm
import os
import argparse
from multiprocessing import Pool
from concurrent.futures import wait

import ctypes
libgcc_s = ctypes.CDLL('libgcc_s.so.1')


def parse_args():
    parser = argparse.ArgumentParser(description="Setup replication experiment")

    parser.add_argument("--src-data-path", default="../fake_imagenet", help="Data to upload to src bucket")

    # gateway path parameters
    parser.add_argument("--src-region", default="aws:us-east-1", help="AWS region of source bucket")
    parser.add_argument("--dest-region", default="aws:us-west-1", help="AWS region of destination bucket")

    # bucket namespace
    parser.add_argument("--bucket-prefix", default="sarah", help="Prefix for bucket to avoid naming collision")
    parser.add_argument("--key-prefix", default="", help="Prefix keys")

    # gateway provisioning
    parser.add_argument("--gcp-project", default=None, help="GCP project ID")
    parser.add_argument("--azure-subscription", default=None, help="Azure subscription")
    parser.add_argument("--gateway-docker-image", default="ghcr.io/parasj/skylark:main", help="Docker image for gateway instances")
    args = parser.parse_args()

    return args

def upload(region, bucket, path, key):
    obj_store = ObjectStoreInterface.create(region, bucket)
    obj_store.upload_object(path, key).result()


def main(args):
    src_bucket = f"{args.bucket_prefix}-skylark-{args.src_region.split(':')[1]}"
    dst_bucket = f"{args.bucket_prefix}-skylark-{args.dest_region.split(':')[1]}"
    os.system(f"SRC_BUCKET={src_bucket}")
    os.system(f"DEST_BUCKET={dst_bucket}")
    print(src_bucket)
    print(dst_bucket)
    obj_store_interface_src = ObjectStoreInterface.create(args.src_region, src_bucket)
    obj_store_interface_src.create_bucket()
    obj_store_interface_dst = ObjectStoreInterface.create(args.dest_region, dst_bucket)
    obj_store_interface_dst.create_bucket()

    p = Pool(32*4)
    p.starmap(upload, [(args.src_region, src_bucket, os.path.join(args.src_data_path, f), f"{args.key_prefix}/{f}") for f in os.listdir(args.src_data_path)])
    p.close()

    #futures = []
    #for f in tqdm(os.listdir(args.src_data_path)):
    #    futures.append(
    #        obj_store_interface_src.upload_object(os.path.join(args.src_data_path, f), f"{args.key_prefix}/{f}")
    #    )
    #    if len(futures) > 500: # wait, or else awscrt errors
    #        print("waiting for completion")
    #        wait(futures) 
    #        futures = []


    ## check files

    def done_uploading(): 
        bucket_size = len(list(obj_store_interface_src.list_objects(prefix=args.key_prefix)))
        #f"Length mismatch {len(os.listdir(args.src_data_path))}, {bucket_size}"
        print("bucket", bucket_size, len(os.listdir(args.src_data_path)))
        return len(os.listdir(args.src_data_path)) == bucket_size

    wait_for(done_uploading, timeout=60, interval=0.1, desc=f"Waiting for files to upload")

    #for f in tqdm(os.listdir(args.src_data_path)):
    #    assert obj_store_interface_src.exists(f"{args.key_prefix}/{f}")







if __name__ == "__main__":
    main(parse_args())
 
