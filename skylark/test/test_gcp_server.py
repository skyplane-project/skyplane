from loguru import logger

from skylark import skylark_root
from skylark.compute.gcp.gcp_server import GCPServer

if __name__ == "__main__":
    gcp_project = "bair-commons-307400"
    gcp_regions = ["us-central1-f"]

    GCPServer.configure_default_firewall(gcp_project)
    ssh_private_key = skylark_root / "data" / "gcp.pem"
    ssh_public_key = skylark_root / "data" / "gcp.pub"
    ssh_private_key.parent.mkdir(exist_ok=True)
    GCPServer.create_ssh_key(ssh_private_key, ssh_public_key)

    # terminate all instances with tag skylark == true
    compute = GCPServer.get_gcp_client("compute", "v1")
    for zone in gcp_regions:
        instances = compute.instances().list(project=gcp_project, filter="labels.skylark=true", zone=zone).execute()
        for instance in instances.get("items", []):
            zone = instance["zone"].split("/")[-1]
            server = GCPServer(f"gcp:{zone}", gcp_project, instance["name"])
            logger.warning(f"Terminating {server}")
            server.terminate_instance()

    # provision new instances in each region
    for zone in gcp_regions:
        # generate name as UUID
        server = GCPServer.provision_instance(
            zone,
            gcp_project,
            "n1-standard-1",
            premium_network=False,
            ssh_pub_key=ssh_public_key,
        )
        logger.warning(f"Provisioned {server}")
