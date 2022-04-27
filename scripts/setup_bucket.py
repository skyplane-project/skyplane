from skylark.obj_store.object_store_interface import ObjectStoreInterface
from skylark.utils.utils import do_parallel
from skylark.utils import logger
from tqdm import tqdm
import os
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Setup replication experiment")
    parser.add_argument("--src-data-path", default="../fake_imagenet", help="Data to upload to src bucket")
    parser.add_argument("--src-region", default="aws:us-east-1", help="AWS region of source bucket")
    parser.add_argument("--dest-region", default="aws:us-west-1", help="AWS region of destination bucket")
    parser.add_argument("--bucket-prefix", default="sarah", help="Prefix for bucket to avoid naming collision")
    parser.add_argument("--key-prefix", default="", help="Prefix keys")
    args = parser.parse_args()
    return args


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

    # check for read access
    try:
        next(obj_store_interface_src.list_objects(args.key_prefix))
    except StopIteration:
        pass
    except Exception as e:
        logger.error(f"Failed to list objects in source bucket {src_bucket}, do you have read access?: {e}")
        exit(1)

    # query for all keys under key_prefix
    objs = {obj.key: obj.size for obj in obj_store_interface_src.list_objects(args.key_prefix)}
    fn_args = []
    for f in tqdm(os.listdir(args.src_data_path)):
        path = os.path.join(args.src_data_path, f)
        key = f"{args.key_prefix}/{f}"
        if key not in objs.keys() or objs[key] != os.path.getsize(path):
            fn_args.append((path, key))
    print(f"Found {len(fn_args)} files to upload")
    upload_fn = lambda x: obj_store_interface_src.upload_object(x[0], x[1])
    do_parallel(upload_fn, fn_args, n=256, progress_bar=True, desc="Uploading", arg_fmt=lambda x: x[1])

    # check all objects uploaded
    objs = {obj.key: obj.size for obj in obj_store_interface_src.list_objects(args.key_prefix)}
    assert all(x[1] in objs for x in fn_args)


if __name__ == "__main__":
    main(parse_args())
