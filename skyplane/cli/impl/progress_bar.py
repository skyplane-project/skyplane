import skyplane
from typing import List
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from skyplane import exceptions
from skyplane.chunk import Chunk
from skyplane.cli.impl.common import console, print_stats_completed
from skyplane.utils.definitions import format_bytes


class ProgressBarTransferHook(skyplane.TransferHook):
    def on_dispatch_start(self):
        return

    def __init__(self):
        # start spinner
        self.spinner = Progress(
            SpinnerColumn(),
            TextColumn("Dispatching chunks...{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            transient=True,
        )
        self.pbar = None
        self.transfer_task = None
        self.chunks_dispatched = 0
        self.chunks_completed = 0
        self.bytes_dispatched = 0
        self.bytes_completed = 0
        self.dispatch_task = self.spinner.add_task("", total=None)
        self.spinner.start()

    def on_chunk_dispatched(self, chunks: List[Chunk]):
        # update bytes_dispatched
        if len(chunks) == 0:
            self.bytes_dispatched = 0
        else:
            self.bytes_dispatched += sum([chunk.chunk_length_bytes for chunk in chunks])
            self.chunks_dispatched += len(chunks)
        # rerender spinners with updated text "Dispatching chunks (~{format_bytes(self.bytes_dispatched)} dispatched)"
        self.spinner.update(self.dispatch_task, description=f" (~{format_bytes(self.bytes_dispatched)} dispatched)")

    def on_dispatch_end(self):
        self.spinner.stop()
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

    def on_chunk_completed(self, chunks: List[Chunk]):
        if len(chunks) == 0:
            self.bytes_completed = 0
        else:
            self.chunks_completed += len(chunks)
            self.bytes_completed += sum([chunk.chunk_length_bytes for chunk in chunks])
        self.pbar.update(self.transfer_task, completed=self.bytes_completed)

    def on_transfer_end(self, transfer_stats):
        self.pbar.stop()
        print_stats_completed(total_runtime_s=transfer_stats["total_runtime_s"], throughput_gbits=transfer_stats["throughput_gbits"])

    def on_transfer_error(self, error):
        console.log(error)
        raise exceptions.SkyplaneGatewayException("Transfer failed with error", error)
