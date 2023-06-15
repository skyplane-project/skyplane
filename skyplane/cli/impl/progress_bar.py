from typing import List, Optional
from collections import defaultdict
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from skyplane import exceptions
from skyplane.chunk import Chunk
from skyplane.cli.impl.common import console, print_stats_completed
from skyplane.utils.definitions import format_bytes
from skyplane.api.tracker import TransferHook


class ProgressBarTransferHook(TransferHook):
    """Transfer hook for multi-destination transfers."""

    def on_dispatch_start(self):
        return

    def __init__(self, dest_region_tags: List[str]):
        self.spinner = Progress(
            SpinnerColumn(),
            TextColumn("Dispatching chunks...{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            transient=True,
        )
        self.dest_region_tags = dest_region_tags
        self.pbar = None  # map between region_tag and progress bar
        self.transfer_task = {}
        self.chunks_dispatched = 0
        self.bytes_dispatched = 0
        self.chunks_completed = defaultdict(int)
        self.bytes_completed = defaultdict(int)
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
        self.spinner.update(
            self.dispatch_task, description=f" {self.chunks_dispatched} chunks (~{format_bytes(self.bytes_dispatched)} dispatched)"
        )

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
        for region_tag in self.dest_region_tags:
            self.transfer_task[region_tag] = self.pbar.add_task(region_tag, total=self.bytes_dispatched)
        self.pbar.start()

    def on_chunk_completed(self, chunks: List[Chunk], region_tag: Optional[str] = None):
        assert region_tag is not None, f"Must specify region tag for progress bar"
        self.chunks_completed[region_tag] += len(chunks)
        self.bytes_completed[region_tag] += sum([chunk.chunk_length_bytes for chunk in chunks])
        self.pbar.update(self.transfer_task[region_tag], completed=self.bytes_completed[region_tag])

    def on_transfer_end(self):
        self.pbar.stop()

    def on_transfer_error(self, error):
        console.log(error)
        raise exceptions.SkyplaneGatewayException("Transfer failed with error", error)
