import time
from skyplane.api.impl.path import parse_path
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.definitions import MB
import argparse
import os

FILE_SIZE = 500 * MB

def test_setup(provider):
    # Uploading file to read from
    os.system(f"dd if=/dev/zero of=test_local1.txt bs={FILE_SIZE} count=1")

    print("Uploading file to read from...")
    provider.upload_object("test_local1.txt", "test1.txt")
    assert provider.exists("test1.txt")

def test_cleanup(provider):
    provider.delete_objects(["test1.txt"])
    os.remove("test_local1.txt")

def test_read(provider):
    # Reading file. Reading test1.txt from the source to test.txt
    print("Reading file...")
    before= time.time()
    assert provider.exists("test1.txt")
    print("File exists")
    assert provider.download_object("test1.txt", "test_local2.txt")
    after = time.time()
    transfer_time = after-before
    print(f"Test read only finished. Time taken: {transfer_time}, Throughput: {transfer_time/(FILE_SIZE/MB)}MBps")

def test_write(provider, destination):

    # Uploading file to read from. Writing test.txt to the destination test2.txt
    print("Writing file...")
    before= time.time()
    destination.upload_object("test.txt", "test2.txt")
    after = time.time()
    transfer_time = after-before

    print(f"Test read write finished. Time taken: {transfer_time}, Throughput: {transfer_time/(FILE_SIZE/MB)}MBps")

    destination.delete_objects(["test2.txt"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dst", type=str, required=False)
    args = parser.parse_args()

    read_only = False

    src, src_host, src_path = parse_path(args.src)
    print("Printing", src, src_host, src_path)
    provider = ObjectStoreInterface.create(src, src_host)

    # #create local file
    test_setup(provider)
    if args.dst:
        dst, dst_host, dst_path = parse_path(args.dst)
        destination = ObjectStoreInterface.create(dst, dst_host)

    #test read and write here
    before = time.time()
    test_read(provider)
    if args.dst:
        test_write(provider, destination)
    after = time.time()

    print(f"All tests finished. Time taken: {after-before}, Throughput: {(after-before)/FILE_SIZE}MBps")
    test_cleanup(provider)