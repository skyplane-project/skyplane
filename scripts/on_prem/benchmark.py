import os
import time
from pyarrow import fs
import argparse
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

KB = 1024
MB = 1024 * 1024
GB = MB * 1024

THREADS = 32

def transfer_file(in_fs, in_path, out_fs, out_path, BATCH_SIZE, final):
    print("Starting transfer...")
    if out_fs is not None:
        with in_fs.open_input_stream(in_path) as in_file:
            with out_fs.open_output_stream(out_path) as out_file:
                while True:
                    buf = in_file.read(BATCH_SIZE)
                    if buf:
                        out_file.write(buf)
                    else:
                        break
    else:
        in_file = in_fs.open_input_stream(in_path)
        curr = 0
        while curr < final:
            buf = in_file.read(BATCH_SIZE)
            #print(f"Reading!{threading.get_ident()}", flush=True)
            curr += BATCH_SIZE
            if not buf:
                break


def setup_files_and_dirs(outdir, hdfs):
    # setup 10GB file
    hdfs.create_dir(f"/data")
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    os.system(f"dd if=/dev/zero of={outdir}/10GBdata.bin bs=128KB count=78125")


def cleanup_files_and_dirs(outdir, hdfs):
    # setup 10GB file
    hdfs.delete_dir(f"/data")
    os.system(f"rm -rf {outdir}")


def transfer_local_to_hdfs(hdfs, local, outdir):
    # 32/64/128/156 MBs
    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 32 * MB)

    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 64 * MB)

    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 128 * MB)

    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 156 * MB)


def transfer_hdfs_to_local(hdfs, local, outdir):
    # 32/64/128/156 MBs
    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 32 * MB)

    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 64 * MB)

    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 128 * MB)

    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 156 * MB)


def parallel_reads(args):
    (hdfs, final) = args
    new_hdfs = fs.HadoopFileSystem(host=hdfs, port=8020, extra_conf={"dfs.client.use.datanode.hostname": "false"})
    transfer_file(new_hdfs, f"/data/10GBdata.bin", None, f"data/10GBdata.bin", 128 * MB, final)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("HDFS", type=str, help="HDFS host")
    parser.add_argument("--outdir", type=str, default="/tmp/data")
    parser.add_argument("--threads", type=bool, default=True)
    args = parser.parse_args()

    hdfs = fs.HadoopFileSystem(host=args.HDFS, port=8020, user="hadoop", extra_conf={"dfs.client.use.datanode.hostname": "false"})
    local = fs.LocalFileSystem()
    thread = args.threads
    #setup_files_and_dirs(args.outdir, hdfs)
    #transfer_local_to_hdfs(hdfs, local, args.outdir)
    #transfer_hdfs_to_local(hdfs, local)
    arg = []
    increment = 10*GB/THREADS
    curr = 0
    multiprocessing.set_start_method("spawn")
    #prepare args
    for i in range(THREADS):
        arg.append((args.HDFS, increment))
        curr += increment
    if (thread):
    #execute the threads
        with ThreadPoolExecutor(max_workers=THREADS) as p:
            before = time.time()
            future = [p.submit(parallel_reads, arg[i]) for i in range(THREADS)]
            #p.map(parallel_reads, args)
    else:
        with ProcessPoolExecutor(max_workers=THREADS) as p:
            before = time.time()
            future = [p.submit(parallel_reads, arg[i]) for i in range(THREADS)]
            #print(future.result())
            #p.map(example, args)


    print(f"Finished! Time:{time.time()-before}")
    """
    t1 = threading.Thread(target=parallel_reads, args=(hdfs, lock, sema))
    t2 = threading.Thread(target=parallel_reads, args=(hdfs, lock, sema))
    t1.start()
    t2.start()
    t2.join()"""