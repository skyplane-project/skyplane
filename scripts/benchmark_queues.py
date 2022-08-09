"""
Script to compare performance of multiprocessing.Queue versus multiprocessing.Manager.Queue. This script
will fork 32 processes. Processes 0-15 will enqueue 1024 large objects to a multiprocessing.Queue. Processes
16-31 will read from the same multiprocessing.Queue until all objects have been read. Processes 16-31 will
share a multiprocessing.Manager.Value to track the number of objects remaining to be read (made threadsafe
by locking.
"""

from multiprocessing import Process, Manager, Value, Queue
from queue import Empty
import time
import os
import argparse

N_MSG_PER_PROCESS = 512
MSG_SIZE = 64 * 1024


def writer_process(queue):
    for i in range(N_MSG_PER_PROCESS):
        # make 1KB random data
        data = os.urandom(MSG_SIZE)
        queue.put(data)
    print(f"DONE: {N_MSG_PER_PROCESS} objects enqueued")


def reader_process(queue, value):
    while True:
        # check if we're done
        with value.get_lock():
            # print(f"{value.value} objects processed")
            if value.value >= N_MSG_PER_PROCESS * 16:
                break

        # get data using non-blocking get
        try:
            queue.get_nowait()
            with value.get_lock():
                value.value += 1
        except Empty:
            continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["queue", "manager", "thread"], default="queue")
    args = parser.parse_args()

    if args.mode == "queue":
        queue = Queue()
        value = Value("i", 0)
    else:
        manager = Manager()
        queue = manager.Queue()
        value = Value("i", 0)

    start_time = time.time()

    # start writer processes
    writer_processes = []
    for i in range(16):
        p = Process(target=writer_process, args=(queue,))
        p.start()
        writer_processes.append(p)

    # start reader processes
    reader_processes = []
    for i in range(16):
        p = Process(target=reader_process, args=(queue, value))
        p.start()
        reader_processes.append(p)

    # wait for all processes to finish
    for p in writer_processes + reader_processes:
        p.join()

    print(f"Time: {time.time() - start_time:.2f}s")
