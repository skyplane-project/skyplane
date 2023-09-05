import uuid
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils import logger

def provision_vm():
    vm = GCPCloudProvider().provision_instance("us-east1", "n2-standard-2")
    return vm


def test_vm_simple():
    # provision vm
    vm = provision_vm()

    # create iface
    region = vm.region_tag
    vm_host = "skyplane"
    vm_private_key_path = vm.ssh_private_key
    
    vm_iface = VMInterface(vm_host, vm.gcp_instance_name, vm_region, vm_private_key_path)

    # test a provisioned vm exists
    assert vm_iface.exists()

    # test basic transfer
    assert interface_test_from_iface(vm_iface)
    
