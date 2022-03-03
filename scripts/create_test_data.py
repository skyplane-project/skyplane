from skylark import GB, MB, print_header
import shutil
import os
import argparse
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description="Run a replication job")
    parser.add_argument("--chunk-size-mb", default=128, type=int, help="Chunk size in MB")
    parser.add_argument("--n-chunks", default=16, type=int, help="Number of chunks in bucket")
    parser.add_argument("--directory", default=os.path.expanduser("~/test-data"), help="Test data directory")
    args = parser.parse_args()
    return args


def main(args):
    directory = f"{args.directory}/{args.chunk_size_mb}_{args.n_chunks}/"

    if os.path.isdir(directory):
        shutil.rmtree(directory)
    os.mkdir(directory)

    for chunk in tqdm(range(args.n_chunks)):
        with open(f"{directory}/chunk_{chunk}", "wb") as f:
            f.write(os.urandom(int(MB * args.chunk_size_mb)))


if __name__ == "__main__":
    print_header()
    main(parse_args())
