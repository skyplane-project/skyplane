from skyplane.api.tracker import TransferProgressTracker
from rich.progress import Progress, SpinnerColumn, TextColumn
from skyplane.cli.common import print_stats_completed
from skyplane.utils.definitions import GB
from skyplane.utils.timer import Timer

class SimpleReporter:
    def __init__(self, tracker: TransferProgressTracker):
        self.tracker = tracker

    def update(self):
        with Timer() as t, Progress(SpinnerColumn(), TextColumn("Querying chunks"), transient=True) as progress:
            copy_task = progress.add_task("", total=None)
            # update progress bar
            completed_bytes = self.tracker.query_bytes_dispatched()
            throughput_gbits = completed_bytes * 8 / GB / t.elapsed if t.elapsed > 0 else 0.0
            # make log line
            progress.update(
                copy_task,
                description=f" ({len(self.tracker.job_complete_chunk_ids)} of {len(self.tracker.job_chunk_requests)} chunks)",
                completed=completed_bytes,
            )
            bytes_remaining = self.tracker.query_bytes_remaining()
            if bytes_remaining is not None and bytes_remaining > 0:
                print(f"{(bytes_remaining / (2 ** 30)):.2f}GB left")
                return True
            else:
                print_stats_completed(t.elapsed, (self.tracker.query_bytes_dispatched() / GB * 8) / t.elapsed)
                return False
