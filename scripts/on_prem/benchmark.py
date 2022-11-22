import os
import time
from pyarrow import fs
import threading
import argparse

KB = 1024
def transfer_file(in_fs, in_path, out_fs, out_path, BATCH_SIZE):
    before = time.time()
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
        with in_fs.open_input_stream(in_path) as in_file:
            while True:
                buf = in_file.read(BATCH_SIZE)
                if not buf:
                    break
                
    print(f"Time taken to copy 100 125MB files from local to HDFS for {BATCH_SIZE/KB}KB: {time.time() - before}")
                
def setup_files_and_dirs(outdir):
    #setup 10GB file
    os.mkdir(f"{outdir}")
    os.system(f"dd if=/dev/zero of={outdir}/10GBdata.bin bs=128KB count=78125")
    
def transfer_local_to_hdfs(hdfs, local, outdir):
    #32/64/128/156 KBs
    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 32*KB)
    
    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 64*KB)

    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 128*KB)

    transfer_file(local, f"{outdir}/10GBdata.bin", hdfs, f"/data/10GBdata.bin", 156*KB)

    
def transfer_hdfs_to_local(hdfs, local, outdir):
    #32/64/128/156 KBs
    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 32*KB)
    
    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 64*KB)
    
    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 128*KB)
    
    transfer_file(hdfs, f"/data/10GBdata.bin", local, f"{outdir}/10GBdata.bin", 156*KB)


def parallel_reads(hdfs):
    transfer_file(hdfs, f"/data/10GBdata.bin", None, f"data/10GBdata.bin", 128*KB)
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('HDFS', type=str, help="HDFS host")
    parser.add_argument("--outdir", type=str, default="/tmp/data")
    args = parser.parse_args()
    
    hdfs = fs.HadoopFileSystem(host=args.HDFS, port=8020, extra_conf={'dfs.client.use.datanode.hostname': 'false'})
    local = fs.LocalFileSystem()

    setup_files_and_dirs(args.outdir)
    transfer_local_to_hdfs(hdfs, local)
    transfer_hdfs_to_local(hdfs, local)
    
    t1 = threading.Thread(target=parallel_reads, args=(hdfs, ))
    t2 = threading.Thread(target=parallel_reads, args=(hdfs, ))
    t1.start()
    t2.start()
