"""
generate_chunks.py: Generate N chunks of fixed size and upload them to S3.
python skylark/benchmark/replicate/generate_chunks.py --region us-east-1 --size_mb 128 --num_chunks 64
"""

import argparse
import os
import random
import time
import tempfile

from loguru import logger
from tqdm import tqdm, trange

from skylark.replicate.s3_interface import S3Interface
from skylark.utils import do_parallel, Timer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", type=str, required=True)
    parser.add_argument("--size_mb", type=int, default=128)
    parser.add_argument("--num_chunks", type=int, default=64)
    return parser.parse_args()


def make_random_file(out_fname, size_mb, batch_size_mb=4):
    assert size_mb % batch_size_mb == 0, f"Size {size_mb} is not a multiple of batch size {batch_size_mb}"
    with open(out_fname, "wb") as f:
        for i in trange(size_mb // batch_size_mb, leave=False, unit="MB", desc="Generate random file"):
            f.write(os.urandom(1000 * 1000 * batch_size_mb))
    logger.info(f"Generated random file {out_fname} of size {os.path.getsize(out_fname) // (1000 * 1000)}MB")


def main(args):
    bucket = f"skylark-{args.region}"
    key_pattern = lambda obj_id: f"/random/{args.size_mb}/{obj_id:05}"
    s3_interface = S3Interface(args.region, bucket)
    s3_interface.create_bucket()

    def get_missing_objects():
        objs = list(s3_interface.list_objects())
        valid_objs = []
        for obj_id in range(args.num_chunks):
            key = key_pattern(obj_id)
            matching = [o for o in objs if "/" + o["Key"] == key]
            if len(matching) == 1:
                size = int(matching[0]["Size"]) / (1000.0 * 1000.0)
                if abs(size - args.size_mb) < 1:
                    valid_objs.append(obj_id)
        missing_objs = [obj_id for obj_id in range(args.num_chunks) if obj_id not in valid_objs]
        return missing_objs

    missing_objs = get_missing_objects()
    logger.info(f"Missing objects: {missing_objs}")

    if len(missing_objs) == 0:
        logger.info("All objects are present. Exiting.")
        return

    with tempfile.NamedTemporaryFile() as f:
        make_random_file(f.name, args.size_mb)
        logger.info(f"Generated random file {f.name}")
        # for obj_id in tqdm(missing_objs, desc="Uploading objects"):
        #     s3_interface.upload_object(f.name, key_pattern(obj_id)).result()
        upload = lambda obj_id: s3_interface.upload_object(f.name, key_pattern(obj_id)).result()
        with Timer() as t:
            do_parallel(upload, missing_objs, progress_bar=True, desc="Uploading objects", arg_fmt=key_pattern, n=16)
        gbps = len(missing_objs) * args.size_mb * 8.0 / 1e3 / t.elapsed
        logger.info(
            f"Uploaded {len(missing_objs)} objects (total = {len(missing_objs) * args.size_mb / 1e3:.2f}GB) in {t.elapsed:.2f}s, ~{gbps:.2f}Gbps"
        )

    logger.info(f"Uploaded {args.num_chunks} objects")
    missing_objs = get_missing_objects()
    assert not missing_objs, f"Not all objects were uploaded: {missing_objs}"


if __name__ == "__main__":
    main(parse_args())
