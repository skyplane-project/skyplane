import time
from skyplane.api.dataplane import TransferProgressTracker

class SimpleReporter:
    def __init__(
        self,
        tracker: TransferProgressTracker
    ):
        self.tracker = tracker

    def update(self):
        bytes_remaining = self.tracker.query_bytes_remaining()

        if bytes_remaining > 0:
            print(f"{(bytes_remaining / (2 ** 30)):.2f}GB left")
            return True
        else:
            return False
    
