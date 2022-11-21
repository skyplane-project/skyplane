import datetime
import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from enum import Enum, auto
from pathlib import Path

import requests
import typer
from rich import print as rprint
from typing import Optional, Dict

import skyplane
import skyplane.cli.usage.definitions
from skyplane.utils.definitions import tmp_log_dir
from skyplane.config import _map_type
from skyplane.config_paths import config_path, cloud_config
from skyplane.replicate.replicator_client import TransferStats
from skyplane.utils import logger


def _get_current_timestamp_ns():
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9)


class UsageStatsStatus(Enum):
    ENABLED_EXPLICITLY = auto()
    DISABLED_EXPLICITLY = auto()
    ENABLED_BY_DEFAULT = auto()


@dataclass
class UsageStatsToReport:
    """Usage stats to report"""

    #: The Skyplane version in use.
    skyplane_version: str
    #: The Python version in use.
    python_version: str
    #: The schema version of the report.
    schema_version: str
    #: The client id from SkyplaneConfig.
    client_id: Optional[str]
    #: A random id of the transfer session.
    session_id: str
    #: The source region of the transfer session.
    source_region: str
    #: The destination region of the transfer session.
    destination_region: str
    #: The source cloud provider of the transfer session.
    source_cloud_provider: str
    #: The destination cloud provider of the transfer session.
    destination_cloud_provider: str
    #: The operating system in use.
    os: str
    #: When the transfer is started.
    session_start_timestamp_ms: int
    #: The collection of command arguments used in the transfer session.
    arguments_dict: Optional[Dict] = None
    #: The collection of transfer stats upon completion of the transfer session.
    transfer_stats: Optional[TransferStats] = None
    #: The collection of error message and the function responsible if the transfer fails.
    error_dict: Optional[Dict] = None
    #: The time of the log sent to Loki. It is None if not sent and stored locally in /tmp.
    sent_time: Optional[int] = None


class UsageClient:
    """The client implementation for usage report.
    It is in charge of writing usage stats to the /tmp directory
    and report usage stats.
    """

    def __init__(self, client_id: Optional[str] = cloud_config.anon_clientid):
        self.client_id = client_id
        self.session_id = str(uuid.uuid4())

    @classmethod
    def enabled(cls):
        return cls.usage_stats_status() is not UsageStatsStatus.DISABLED_EXPLICITLY

    @classmethod
    def log_exception(
        cls,
        location: str,
        exception: Exception,
        args: Optional[Dict] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
    ):
        if cls.enabled():
            client = cls()
            error_dict = {"loc": location, "message": str(exception)[:150]}
            stats = client.make_error(
                error_dict=error_dict, arguments_dict=args, src_region_tag=src_region_tag, dest_region_tag=dest_region_tag
            )
            destination = client.write_usage_data(stats)
            client.report_usage_data("error", stats, destination)

    @classmethod
    def log_transfer(
        cls,
        transfer_stats: Optional[TransferStats],
        args: Optional[Dict] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
    ):
        if cls.enabled():
            client = cls()
            stats = client.make_stat(
                arguments_dict=args, transfer_stats=transfer_stats, src_region_tag=src_region_tag, dest_region_tag=dest_region_tag
            )
            destination = client.write_usage_data(stats)
            client.report_usage_data("usage", stats, destination)

    @classmethod
    def usage_stats_status(cls) -> UsageStatsStatus:
        # environment vairable has higher priority
        usage_stats_enabled_env_var = os.getenv(skyplane.cli.usage.definitions.USAGE_STATS_ENABLED_ENV_VAR)
        if usage_stats_enabled_env_var is None:
            pass
        elif not _map_type(usage_stats_enabled_env_var, bool):
            return UsageStatsStatus.DISABLED_EXPLICITLY
        elif _map_type(usage_stats_enabled_env_var, bool):
            return UsageStatsStatus.ENABLED_EXPLICITLY
        elif usage_stats_enabled_env_var is not None:
            raise ValueError(
                f"Valid value for {skyplane.cli.usage.definitions.USAGE_STATS_ENABLED_ENV_VAR} "
                f"env var is (false/no/0) or (true/yes/1), but got {usage_stats_enabled_env_var}"
            )
        # then check in the config file
        usage_stats_enabled_config_var = None
        # TODO: Check the correct error
        try:
            usage_stats_enabled_config_var = cloud_config.get_flag("usage_stats")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.debug(f"Failed to load usage stats config {e}")

        if usage_stats_enabled_config_var is None:
            pass
        elif not usage_stats_enabled_config_var:
            return UsageStatsStatus.DISABLED_EXPLICITLY
        elif usage_stats_enabled_config_var:
            return UsageStatsStatus.ENABLED_EXPLICITLY
        elif usage_stats_enabled_config_var is not None:
            raise ValueError(
                f"Valid value for 'usage_stats' in {config_path}"
                f" is (false/no/0) or (true/yes/1), but got {usage_stats_enabled_config_var}"
            )

        return UsageStatsStatus.ENABLED_BY_DEFAULT

    @classmethod
    def set_usage_stats_via_config(cls, value, config):
        current_status = cls.usage_stats_status()
        if current_status is UsageStatsStatus.DISABLED_EXPLICITLY:
            if (isinstance(value, bool) and not value) or (isinstance(value, str) and not _map_type(value, bool)):
                rprint("Usage stats collection is already disabled.")
                rprint(skyplane.cli.usage.definitions.USAGE_STATS_REENABLE_MESSAGE)
                return
        elif current_status is UsageStatsStatus.ENABLED_EXPLICITLY:
            if (isinstance(value, bool) and value) or (isinstance(value, str) and _map_type(value, bool)):
                rprint("Usage stats collection is already enabled.")
                rprint(skyplane.cli.usage.definitions.USAGE_STATS_REENABLED_MESSAGE)
                return

        if (isinstance(value, bool) and not value) or (isinstance(value, str) and not _map_type(value, bool)):
            prompt = "Would you still like to opt out of sharing anonymous usage metrics?"
            rprint(skyplane.cli.usage.definitions.USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE + "\n")
            answer = typer.confirm(prompt, default=False)
            if not answer:
                # Do nothing if the user confirms not to disable
                return
            else:
                rprint(skyplane.cli.usage.definitions.USAGE_STATS_REENABLE_MESSAGE)

        try:
            config.set_flag("usage_stats", value)
        except Exception as e:
            raise Exception("Failed to enable/disable by writing to" f"{config_path}") from e

        if config.get_flag("usage_stats"):
            rprint(skyplane.cli.usage.definitions.USAGE_STATS_REENABLED_MESSAGE)

    def make_stat(
        self,
        arguments_dict: Optional[Dict] = None,
        transfer_stats: Optional[TransferStats] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
    ):
        if src_region_tag is None:
            src_provider, src_region = None, None
        else:
            src_provider, src_region = src_region_tag.split(":")
        if dest_region_tag is None:
            dest_provider, dest_region = None, None
        else:
            dest_provider, dest_region = dest_region_tag.split(":")

        return UsageStatsToReport(
            skyplane_version=skyplane.__version__,
            python_version=".".join(map(str, sys.version_info[:3])),
            schema_version=skyplane.cli.usage.definitions.SCHEMA_VERSION,
            client_id=self.client_id,
            session_id=self.session_id,
            source_region=src_region,
            destination_region=dest_region,
            source_cloud_provider=src_provider,
            destination_cloud_provider=dest_provider,
            os=sys.platform,
            session_start_timestamp_ms=int(time.time() * 1000),
            arguments_dict=arguments_dict,
            transfer_stats=transfer_stats,
        )

    def make_error(
        self,
        error_dict: Dict,
        arguments_dict: Optional[Dict] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
    ):
        if src_region_tag is None:
            src_provider, src_region = None, None
        else:
            src_provider, src_region = src_region_tag.split(":")
        if dest_region_tag is None:
            dest_provider, dest_region = None, None
        else:
            dest_provider, dest_region = dest_region_tag.split(":")

        return UsageStatsToReport(
            skyplane_version=skyplane.__version__,
            python_version=".".join(map(str, sys.version_info[:3])),
            schema_version=skyplane.cli.usage.definitions.SCHEMA_VERSION,
            client_id=self.client_id,
            session_id=self.session_id,
            source_region=src_region,
            destination_region=dest_region,
            source_cloud_provider=src_provider,
            destination_cloud_provider=dest_provider,
            os=sys.platform,
            session_start_timestamp_ms=int(time.time() * 1000),
            arguments_dict=arguments_dict,
            error_dict=error_dict,
        )

    def write_usage_data(self, data: UsageStatsToReport, dir_path: Optional[Path] = None):
        """Write the usage data to the directory.
        Params:
            data: Data to report
            dir_path: The path to the directory to write usage data.
        Return:
            destination: The absolute path of the usage data json file.
        """
        if dir_path is None:
            client_id_path = self.client_id if self.client_id else "unknown"
            dir_path = tmp_log_dir / "usage" / client_id_path / str(self.session_id)
        dir_path = Path(dir_path)
        dir_path.mkdir(exist_ok=True, parents=True)
        destination = dir_path / skyplane.cli.usage.definitions.USAGE_STATS_FILE

        with open(destination, "w+") as json_file:
            json_file.write(json.dumps(asdict(data)))
        return destination

    def report_usage_data(self, type: str, data: UsageStatsToReport, path: Path) -> None:
        """Report the usage data to the usage server.
        Params:
            data: Data to report.
        Raises:
            requests.HTTPError if requests fails.
        """

        prom_labels = {"type": type, "environment": "prod"}
        headers = {"Content-type": "application/json"}
        data.sent_time = int(time.time() * 1000)
        payload = {"streams": [{"stream": prom_labels, "values": [[str(_get_current_timestamp_ns()), json.dumps(asdict(data))]]}]}
        payload = json.dumps(payload)
        r = requests.post(skyplane.cli.usage.definitions.LOKI_URL, headers=headers, data=payload, timeout=0.5)

        if r.status_code == 204:
            with open(path, "w") as json_file:
                json_file.write(json.dumps(asdict(data)))
