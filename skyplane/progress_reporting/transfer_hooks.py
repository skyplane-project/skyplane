from abc import ABC
from typing import Dict, List, Set
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from skyplane import exceptions
from skyplane.chunk import ChunkRequest
from skyplane.cli.common import console, print_stats_completed


class TransferHook(ABC):
    def on_dispatch_start(self):
        raise NotImplementedError()

    def on_chunk_dispatched(self, chunks: Dict[str, List[ChunkRequest]]):
        raise NotImplementedError()

    def on_dispatch_end(self):
        raise NotImplementedError()

    def on_chunk_completed(self, chunks: Dict[str, List[ChunkRequest]], complete_chunk_ids: Dict[str, Set[str]]):
        raise NotImplementedError()

    def on_transfer_end(self, transfer_stats):
        raise NotImplementedError()

    def on_transfer_error(self, error):
        raise NotImplementedError()


class EmptyTransferHook(TransferHook):
    def __init__(self):
        return

    def on_dispatch_start(self):
        return

    def on_chunk_dispatched(self, chunks: Dict[str, List[ChunkRequest]]):
        return

    def on_dispatch_end(self):
        return

    def on_chunk_completed(self, chunks: Dict[str, List[ChunkRequest]], complete_chunk_ids: Dict[str, Set[str]]):
        return

    def on_transfer_end(self, transfer_stats):
        return

    def on_transfer_error(self, error):
        return


class ProgressBarTransferHook(TransferHook):
    def __init__(self):
        # start spinner
        self.spinner = Progress(SpinnerColumn(), TextColumn("Dispatching chunks..."), transient=True)
        self.pbar = None
        self.chunks_dispatched = 0
        self.chunks_completed = 0
        self.bytes_dispatched = 0
        self.bytes_completed = 0
        self.dispatch_task = self.spinner.add_task("", total=None)
        self.spinner.start()

    def on_chunk_dispatched(self, chunks: Dict[str, List[ChunkRequest]]):
        # update bytes_dispatched
        if len(chunks) == 0:
            self.bytes_dispatched = 0
        else:
            bytes_total_per_job = {}
            self.chunks_dispatched = 0
            for job_uuid in chunks.keys():
                self.chunks_dispatched += len(chunks[job_uuid])
                bytes_total_per_job[job_uuid] = sum([cr.chunk.chunk_length_bytes for cr in chunks[job_uuid]])
            self.bytes_dispatched = sum(bytes_total_per_job.values())
        # rerender spinner
        self.spinner.update(self.dispatch_task, completed=self.chunks_dispatched)
        self.spinner.refresh()

    def on_dispatch_end(self):
        self.spinner.stop()
        self.spinner.console.print(f"Dispatched {self.chunks_dispatched} chunks")
        self.pbar = Progress(
            SpinnerColumn(),
            TextColumn("Transfer progress{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            transient=True,
        )
        self.transfer_task = self.pbar.add_task("", total=self.bytes_dispatched)
        self.pbar.start()

    def on_chunk_completed(self, chunks: Dict[str, List[ChunkRequest]], complete_chunk_ids: Dict[str, Set[str]]):
        if len(chunks) == 0:
            self.bytes_completed = 0
        else:
            bytes_total_per_job = {}
            self.chunks_completed = 0
            for job_uuid in complete_chunk_ids.keys():
                bytes_total_per_job[job_uuid] = sum(
                    [cr.chunk.chunk_length_bytes for cr in chunks[job_uuid] if cr.chunk.chunk_id in complete_chunk_ids[job_uuid]]
                )
                self.chunks_completed += len(complete_chunk_ids[job_uuid])
            self.bytes_completed = sum(bytes_total_per_job.values())
        # update bytes_completed
        self.pbar.update(
            self.transfer_task, description=f" (Chunk {self.chunks_completed} of {self.chunks_dispatched})", completed=self.bytes_completed
        )
        self.pbar.refresh()

    def on_transfer_end(self, transfer_stats):
        self.pbar.stop()
        print_stats_completed(total_runtime_s=transfer_stats["total_runtime_s"], throughput_gbits=transfer_stats["throughput_gbits"])

    def on_transfer_error(self, error):
        console.log(error)
        raise exceptions.SkyplaneGatewayException("Transfer failed with error", error)
