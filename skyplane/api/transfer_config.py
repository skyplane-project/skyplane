from dataclasses import dataclass


@dataclass(frozen=True)
class TransferConfig:
    autoterminate_minutes: int = 15
    requester_pays: bool = False

    # gateway settings
    use_bbr: bool = True
    use_compression: bool = True
    use_e2ee: bool = True
    use_socket_tls: bool = False

    # provisioning config
    aws_use_spot_instances: bool = False
    azure_use_spot_instances: bool = False
    gcp_use_spot_instances: bool = False
    aws_instance_class: str = "m5.8xlarge"
    azure_instance_class: str = "Standard_D2_v5"
    gcp_instance_class: str = "n2-standard-16"
    gcp_use_premium_network: bool = True

    # multipart config
    multipart_enabled: bool = True
    multipart_threshold_mb: int = 128
    multipart_chunk_size_mb: int = 64
    multipart_max_chunks: int = 10000
