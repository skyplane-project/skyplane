import uuid
import typer
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from skyplane.api.config import TransferConfig
from skyplane.api.provisioner import Provisioner
from skyplane.api.obj_store import ObjectStore
from skyplane.config import SkyplaneConfig
from skyplane.config_paths import config_path
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.obj_store.storage_interface import StorageInterface
from skyplane.api.usage import get_clientid
from skyplane.utils import logger
from skyplane.utils.definitions import tmp_log_dir
from skyplane.utils.path import parse_path

from skyplane.api.pipeline import Pipeline

if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig, TransferConfig, IBMCloudConfig


class SkyplaneClient:
    """Client for initializing cloud provider configurations."""

    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        ibmcloud_config: Optional["IBMCloudConfig"] = None,
        transfer_config: Optional[TransferConfig] = None,
        log_dir: Optional[str] = None,
        disable_aws: bool = False,
        disable_azure: bool = False,
        disable_gcp:  bool = False,
        disable_ibm: bool = False
    ):
        """
        :param aws_config: aws cloud configurations
        :type aws_config: class AWSConfig (optional)
        :param azure_config: azure cloud configurations
        :type azure_config: class AzureConfig (optional)
        :param gcp_config: gcp cloud configurations
        :type gcp_config: class GCPConfig (optional)
        :param ibmcloud_config: ibm cloud configurations
        :type ibmcloud_config: class IBMCloudConfig (optional)
        :param transfer_config: transfer configurations
        :type transfer_config: class TransferConfig (optional)
        :param log_dir: path to store transfer logs
        :type log_dir: str (optional)
        """
        self.config = SkyplaneConfig.default_config()
        if not disable_aws:
            self.config.aws_enabled = True
            if aws_config:
                self.config.aws_access_key = aws_config.aws_access_key
                self.config.aws_secret_key = aws_config.aws_secret_key
        if not disable_azure:
            self.config.azure_enabled = True
            if azure_config:
                self.config.azure_subscription_id=azure_config.azure_subscription_id
                self.config.azure_resource_group=azure_config.azure_resource_group
                self.config.azure_principal_id=azure_config.azure_umi_id
                self.config.azure_umi_name=azure_config.azure_umi_name
                self.config.azure_client_id=azure_config.azure_umi_client_id
        if not disable_gcp:
            self.config.gcp_enabled = True
            if gcp_config:
                self.config.gcp_project_id=gcp_config.gcp_project_id
        if not disable_ibm:
            self.config.ibm_enabled = True
            if ibmcloud_config:
                self.config.ibmcloud_access_id=ibmcloud_config.ibmcloud_access_id
                self.config.ibmcloud_secret_key=ibmcloud_config.ibmcloud_secret_key
                self.config.ibmcloud_iam_key=ibmcloud_config.ibmcloud_iam_key
                self.config.ibmcloud_iam_endpoint=ibmcloud_config.ibmcloud_iam_endpoint
                self.config.ibmcloud_useragent=ibmcloud_config.ibmcloud_useragent
                self.config.ibmcloud_resource_group_id=ibmcloud_config.ibmcloud_resource_group_id

        self.config.to_config_file(config_path)
        typer.secho(f"\nConfig file saved to {config_path}", fg="green")

        self.clientid = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.ibmcloud_auth = ibmcloud_config.make_auth_provider() if ibmcloud_config else None
        self.transfer_config = transfer_config if transfer_config else TransferConfig()
        self.log_dir = (
            tmp_log_dir / "transfer_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if log_dir is None
            else Path(log_dir)
        )

        # set up logging
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "client.log")
        typer.secho(f"Logging to: {self.log_dir / 'client.log'}", fg="bright_black")

        self.provisioner = Provisioner(
            host_uuid=self.clientid,
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
            ibmcloud_auth=self.ibmcloud_auth,
            disable_aws=disable_aws,
            disable_azure=disable_azure,
            disable_gcp=disable_gcp,
            disable_ibm=disable_ibm
        )

    def pipeline(self, planning_algorithm: Optional[str] = "direct", max_instances: Optional[int] = 1, src_iface: Optional[ObjectStoreInterface] = None, dst_ifaces: Optional[List[ObjectStoreInterface]] = None, debug=False):
        """Create a pipeline object to queue jobs"""
        return Pipeline(
            planning_algorithm=planning_algorithm,
            max_instances=max_instances,
            clientid=self.clientid,
            provisioner=self.provisioner,
            transfer_config=self.transfer_config,
            src_iface=src_iface,
            dst_ifaces=dst_ifaces,
            debug=debug,
        )

    def copy(self, src: str, dst: str, recursive: bool = False, max_instances: Optional[int] = 1, debug=False):
        """
        A simple version of Skyplane copy. It automatically waits for transfer to complete
        (the main thread is blocked) and deprovisions VMs at the end.

        :param src: Source prefix to copy from
        :type src: str
        :param dst: The destination of the transfer
        :type dst: str
        :param recursive: If true, will copy objects at folder prefix recursively (default: False)
        :type recursive: bool
        :param max_instances: The maximum number of instances to use per region (default: 1)
        :type max_instances: int
        """
        provider_src, bucket_src, _ = parse_path(src)

        src_iface = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src, aws_auth=self.aws_auth, azure_auth=self.azure_auth, gcp_auth=self.gcp_auth)

        if isinstance(dst, str):
            provider_dst, bucket_dst, _ = parse_path(dst)
            dst_ifaces = [StorageInterface.create(f"{provider_dst}:infer", bucket_dst, aws_auth=self.aws_auth, azure_auth=self.azure_auth, gcp_auth=self.gcp_auth)]
        else:
            dst_ifaces = []
            for dst_path in dst:
                provider_dst, bucket_dst, _ = parse_path(dst_path)
                dst_ifaces.append(StorageInterface.create(f"{provider_dst}:infer", bucket_dst, aws_auth=self.aws_auth, azure_auth=self.azure_auth, gcp_auth=self.gcp_auth))

        pipeline = self.pipeline(max_instances=max_instances, debug=debug, src_iface = src_iface, dst_ifaces=dst_ifaces)
        pipeline.queue_copy(src, dst, recursive=recursive)
        pipeline.start(progress=True)

    def deprovision(self):
        self.provisioner.deprovision()

    def object_store(self):
        """
        Returns an object store interface
        """
        return ObjectStore(
            host_uuid=self.clientid,
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
            ibmcloud_auth=self.ibmcloud_auth
        )
