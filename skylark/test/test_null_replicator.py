import argparse
from skylark.replicate.replication_plan import ReplicationTopology
from skylark.replicate.replicator import ReplicatorCoordinator


def check_pathset(paths, gcp_project, gateway_docker_image):
    topo = ReplicationTopology(paths)
    replicator = ReplicatorCoordinator(
        topology=topo,
        gcp_project=gcp_project,
        gateway_docker_image=gateway_docker_image,
    )
    replicator.provision_gateways()

    # ensure all instances are running w/ hello world docker image
    for path in replicator.bound_paths:
        for server in path:
            out, err = server.run_command("sudo docker run --rm hello-world")
            assert "Hello from Docker!" in out
    replicator.deprovision_gateways()


def test_direct_path():
    check_pathset([["aws:us-east-1", "aws:us-west-1"]], "skylark-333700", "ghcr.io/parasj/skylark:latest")
