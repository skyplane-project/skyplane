"""Generates a large number of small (1KB)."""

import argparse
from functools import partial
from pathlib import Path

import numpy as np

from skyplane.utils.fn import do_parallel


def make_file(data, fname):
    with open(fname, "wb") as f:
        f.write(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nfiles", type=int, default=1024)
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--outdir", type=str, default="/tmp/data")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    files = [f"{outdir}/{i:08d}.bin" for i in range(args.nfiles)]
    data = np.arange(args.size // 4, dtype=np.int32).tobytes()
    do_parallel(
        partial(make_file, data),
        files,
        desc="Generating files",
        spinner=True,
        spinner_persist=True,
    )
