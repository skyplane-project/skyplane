"""
Script to generate random sorted data.

usage: python gen_sorted.py --sizemb 16 --nchunks 1024 --outdir /tmp/data

output:
    /tmp/data/00000.bin
    ...
    /tmp/data/01023.bin

where each file contains a sorted binary list of 4-byte integers that loops.
"""

import argparse
import numpy as np
from pathlib import Path
from multiprocessing import Pool
import tqdm


def make_sorted_segment(n_ints):
    return np.arange(n_ints, dtype=np.int32).tobytes()


def gen_file(chunk_id, size_mb, outdir):
    with open(f"{outdir}/{chunk_id:05d}.bin", "wb") as f:
        bytes_written = 0
        while bytes_written < size_mb * 1024 * 1024:
            batch_size = min(size_mb * 1024 * 1024 - bytes_written, 1024 * 1024) // 4
            f.write(make_sorted_segment(batch_size))
            bytes_written += batch_size * 4


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sizemb", type=int, default=8)
    parser.add_argument("--nchunks", type=int, default=8 * 1024)
    parser.add_argument("--outdir", type=str, default="/tmp/data")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    def worker(chunk_id):
        gen_file(chunk_id, args.sizemb, args.outdir)

    with Pool(processes=16) as pool:
        for _ in tqdm.tqdm(pool.imap_unordered(worker, range(args.nchunks)), total=args.nchunks):
            pass
