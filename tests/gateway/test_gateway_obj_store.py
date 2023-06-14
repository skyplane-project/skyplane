from typing import Any, Iterator, Tuple, Dict, Optional, List
import shutil
from skyplane.api.config import TransferConfig
import time
import docker
import argparse
from skyplane.cli.impl.progress_bar import ProgressBarTransferHook
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.api.config import TransferConfig
from skyplane.utils.fn import PathLike
from skyplane.api.dataplane import Dataplane
import os
from skyplane.compute.server import Server
from skyplane.planner.planner import MulticastDirectPlanner
from skyplane.api.transfer_job import CopyJob, TestCopyJob
from skyplane.api.provisioner import Provisioner
import subprocess

from tests.gateway.test_server import TestServer
from tests.gateway.test_interface import TestInterface


"""

This test module creates local docker containers that run gateway code. Gateways interface with real object stores. 
"""


def wait_container_running(container_name):
    client = docker.from_env()
    while True:
        container = client.containers.get(container_name)
        print("container", container, container.status)
        if container.status == "running":
            return True
        print(f"Waiting for container {container_name} to start")
        time.sleep(1)


def remove_container_if_running(container_name):
    client = docker.from_env()
    if check_container_running(container_name):
        container = client.containers.get(container_name)
        container.stop()
        container.remove()


def check_container_running(container_name):
    # TODO: also check image version that is being run
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        print("container", container, container.status)
        if container.status == "running":
            return True
        else:
            print("removing", container_name)
            container.remove()
            return False
    except docker.errors.NotFound:
        return False


def run(gateway_docker_image: str, restart_gateways: bool):
    """Run the gateway docker image locally"""
    #job = CopyJob("s3://feature-store-datasets/yahoo/processed_yahoo/A1/", ["gs://38046a6749df436886491a95cacdebb8/yahoo/"], recursive=True)
    job = CopyJob("gs://skyplane-test-bucket/files_100000_size_4_mb/", "s3://integrationus-west-2-4450f073/", recursive=True)

    topology = MulticastDirectPlanner(1, 64, TransferConfig()).plan([job])
    print([g.region_tag for g in topology.get_gateways()])
    # print(topology.generate_gateway_program("test:source"))

    source_server = TestServer(topology.src_region_tag, local_port=8081)
    dest_server = TestServer(topology.dest_region_tags[0], local_port=8082)

    test_log_dir = "test_logs/"

    # bind dataplane to docker servers
    dataplane = Dataplane("test", topology, Provisioner("test"), transfer_config=TransferConfig(), log_dir=test_log_dir, local=True)
    dataplane.bound_nodes[topology.get_region_gateways(topology.src_region_tag)[0]] = source_server
    dataplane.bound_nodes[topology.get_region_gateways(topology.dest_region_tags[0])[0]] = dest_server

    # set ip address for docker containers (for gateway info file generation)
    topology.set_ip_addresses(topology.get_region_gateways(topology.src_region_tag)[0].gateway_id, "skyplane_source", "skyplane_source")
    topology.set_ip_addresses(topology.get_region_gateways(topology.dest_region_tags[0])[0].gateway_id, "skyplane_dest", "skyplane_dest")

    # create docker container
    if restart_gateways or not check_container_running("skyplane_source"):
        if check_container_running("skyplane_source"):
            remove_container_if_running("skyplane_source")

        # TODO: set network field
        dataplane._start_gateway(
            gateway_docker_image=gateway_docker_image,
            gateway_node=topology.get_region_gateways(topology.src_region_tag)[0],
            gateway_server=source_server,
            gateway_log_dir=test_log_dir,
            container_name="skyplane_source",
            port=8081,
        )
    if restart_gateways or not check_container_running("skyplane_dest"):
        if check_container_running("skyplane_dest"):
            remove_container_if_running("skyplane_dest")
        dataplane._start_gateway(
            gateway_docker_image=gateway_docker_image,
            gateway_node=topology.get_region_gateways(topology.dest_region_tags[0])[0],
            gateway_server=dest_server,
            gateway_log_dir=test_log_dir,
            container_name="skyplane_dest",
            port=8082,
        )

    # provision dataplane
    dataplane.provisioned = True

    # run dataplane
    try:
        dataplane.run([job], hooks=ProgressBarTransferHook(topology.dest_region_tags))
    except KeyboardInterrupt:
        print("Keyboard interrupt: copying logs...")
    except Exception as e:
        raise e
    finally:
        dataplane.copy_gateway_log(source_server, "skyplane_source")
        dataplane.copy_gateway_log(dest_server, "skyplane_dest")


# check chunk status on destination gateway

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker-image", type=str, required=True)
    parser.add_argument("--restart-gateways", action="store_true", default=False)
    args = parser.parse_args()

    return_code = run(args.docker_image, args.restart_gateways)
    exit(return_code)
