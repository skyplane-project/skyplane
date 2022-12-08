import json
import queue
import socket
import ssl
import time
import traceback
import psutil
from functools import partial
from multiprocessing import Event, Process, Queue

import lz4.frame
import nacl.secret
import urllib3
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.gateway.gateway_sender import GatewaySender
from skyplane.utils import logger
from skyplane.utils.definitions import MB
from skyplane.utils.retry import retry_backoff
from skyplane.utils.timer import Timer


class GatewayOnPrem(GatewaySender):
    def start_workers(self):
        # Assert no other prallel internet connections are active on the node
        # Else this could lead to a noisy neighbor problem
        assert len(psutil.net_connections()) < 5, "Cannot start workers when other workers are running"
        for ip, num_connections in self.outgoing_ports.items():
            for i in range(num_connections):
                p = Process(target=self.worker_loop, args=(i, ip))
                p.start()
                self.processes.append(p)
