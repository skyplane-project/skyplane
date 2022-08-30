import argparse
from math import ceil
from multiprocessing import Pool
from botocore.exceptions import ClientError
import logging
import numpy as np

from skyplane.obj_store.s3_interface import S3Interface

if __name__ == "__main__":
    """
    Format:
        -r : the region of the bucke. Defaults to us-east-2.
        -mb : the total MB. Defaults to 4MB.
        -n : the number of files. Defaults to 1.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", help="The region of the bucke. Defaults to us-east-2.", default='us-east-2')
    parser.add_argument("-mb", help = "Total MB size. Defaults to 4MB.", default=4)
    parser.add_argument("-n", help="the number of files. Defaults to 1.", default=1)
    args = parser.parse_args()
    
    bucket_name = input("Please input the name of bucket: ")
    interface= S3Interface(bucket_name)
    interface.create_bucket(args.r)
    n = int(args.n)
    mb_size = ceil(int(args.mb)/n)
    
    def worker(filenum):
        def make_sorted_segment(n_ints):
            return np.arange(n_ints, dtype=np.int32).tobytes()
        try:
            file_name=f"random-{str(filenum)}"
            with open(f"{file_name}.bin", "wb") as f:
                bytes_written = 0
                while bytes_written < mb_size * 1024 * 1024:
                    batch_size = min(mb_size * 1024 * 1024 - bytes_written, 1024 * 1024) // 4
                    f.write(make_sorted_segment(batch_size))
                    bytes_written += batch_size * 4    
            interface.upload_object(f"{file_name}.bin", file_name)
        except ClientError as e:
            logging.error(e)

    with Pool(processes=16) as pool:
        response = pool.map(worker, range(n))

    delete = input("Press q to delete objects or Q to delete the bucket. Press any other key to exit.")
    if delete == 'q' or 'Q':
        keys = [f"random-{str(filenum)}" for filenum in range(n)]
        interface.delete_objects(keys)
        if delete == 'Q':
            interface.delete_bucket()