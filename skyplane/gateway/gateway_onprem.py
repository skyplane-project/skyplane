import psutil
from multiprocessing import Process

# TODO: migrate to programmable gateways
# from skyplane.gateway.gateway_sender import GatewaySender
#
#
# class GatewayOnPrem(GatewaySender):
#    def start_workers(self):
#        # Assert no other prallel internet connections are active on the node
#        # Else this could lead to a noisy neighbor problem
#        assert len(psutil.net_connections()) < 5, "Cannot start workers when other workers are running"
#        for ip, num_connections in self.outgoing_ports.items():
#            for i in range(num_connections):
#                p = Process(target=self.worker_loop, args=(i, ip))
#                p.start()
#                self.processes.append(p)
#
