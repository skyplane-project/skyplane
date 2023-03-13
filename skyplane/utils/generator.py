from typing import Generator, List, TypeVar
from queue import Queue
import threading


T = TypeVar("T")


def batch_generator(gen_in: Generator[T, None, None], batch_size: int) -> Generator[List[T], None, None]:
    """Batches generator, while handling StopIteration

    :param gen_in: generator that generates chunk requests
    :type gen_in: Generator
    """
    batch = []
    for item in gen_in:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def prefetch_generator(gen_in: Generator[T, None, None], buffer_size: int) -> Generator[T, None, None]:
    """
    Prefetches from generator while handing StopIteration to ensure items yield immediately.
    Start a thread to prefetch items from the generator and put them in a queue. Upon StopIteration,
    the thread will add a sentinel value to the queue.

    :param gen_in: generator that generates chunk requests
    :type gen_in: Generator
    :param buffer_size: maximum size of the buffer to temporarily store the generators
    :type buffer_size: int
    """
    sentinel = object()
    queue = Queue(maxsize=buffer_size)

    def prefetch():
        for item in gen_in:
            queue.put(item)
        queue.put(sentinel)

    thread = threading.Thread(target=prefetch, daemon=True)
    thread.start()

    while True:
        item = queue.get()
        if item is sentinel:
            break
        yield item


def tail_generator(gen_in: Generator[T, None, None], out_list: List[T]) -> Generator[T, None, None]:
    """Tails generator while handling StopIteration

    :param gen_in: generator that generates chunk requests
    :type gen_in: Generator
    :param out_list: list of tail generators
    :type out_list: List
    """
    for item in gen_in:
        out_list.append(item)
        yield item
