import argparse
from skylark.replicate.replication_plan import ReplicationTopology
from skylark.replicate.replicator import ReplicatorCoordinator


def test_pathset(args, paths):
    topo = ReplicationTopology(paths)
    replicator = ReplicatorCoordinator(
        topology=topo,
        gcp_project=args.gcp_project,
        gateway_docker_image=args.gateway_docker_image,
    )
    replicator.provision_gateways()

    # ensure all instances are running w/ hello world docker image
    for path in replicator.bound_paths:
        for server in path:
            out, err = server.run_command("sudo docker run --rm hello-world")
            assert "Hello from Docker!" in out

    replicator.deprovision_gateways()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_project", default="skylark-333700")
    parser.add_argument("--gateway_docker_image", default="ghcr.io/parasj/skylark-docker:latest")
    args = parser.parse_args()

    # single direct path
    test_pathset(args, [["aws:us-east-1", "aws:us-west-1"]])