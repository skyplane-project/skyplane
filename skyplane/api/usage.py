import datetime
import json
import os
import sys
import time
import uuid
import configparser
from dataclasses import asdict, dataclass
from enum import Enum, auto
from pathlib import Path

import requests
from rich import print as rprint
from typing import Optional, Dict

import skyplane
from skyplane.utils.definitions import tmp_log_dir
from skyplane.config import _map_type
from skyplane.config_paths import config_path, cloud_config, host_uuid_path
from skyplane.utils import logger, imports

SCHEMA_VERSION = "0.2"
LOKI_URL = "http://34.212.234.105:9090/loki/api/v1/push"
USAGE_STATS_ENABLED_ENV_VAR = "SKYPLANE_USAGE_STATS_ENABLED"
USAGE_STATS_FILE = "usage_stats.json"
USAGE_STATS_ENABLED_MESSAGE = (
    "[bright_black]To disable performance logging info: https://skyplane.org/en/latest/performance_stats_collection.html[/bright_black]"
)
USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE = (
    "[green][bold]We are an academic research group working to improve inter-cloud network performance.[/bold] "
    "You can inspect what we share in the /tmp/skyplane/metrics directory. "
    "We do not collect any personal data and only collect high-level performance data to improve the accuracy of our solver.[/green]"
    "\n\nSkyplane collects the following anonymous data:"
    "\n    * System and OS information (OS version, kernel version, Python version)"
    "\n    * Anonymized client id and transfer session id"
    "\n    * Source region and destination region per transfer"
    "\n    * The collection of command arguments used in the transfer session"
    "\n    * Total runtime and the aggregated transfer speed in Gbps"
    "\n    * Error message if the transfer fails"
)
USAGE_STATS_REENABLE_MESSAGE = (
    "[yellow][bold]If you want to re-enable usage statistics, run `skyplane config set usage_stats true`.[/bold][/yellow]"
)
USAGE_STATS_REENABLED_MESSAGE = (
    "[green][bold]Thank you for your support of open-source research![/bold][/green]"
    "\nIf you want to disable usage statistics, run `skyplane config set usage_stats false`."
)
USAGE_STATS_DISABLED_MESSAGE = "Usage stats collection is disabled."


def get_clientid():
    path = host_uuid_path
    config = configparser.ConfigParser()
    if path.exists():
        config.read(os.path.expanduser(path))
    id = uuid.UUID(int=uuid.getnode()).hex
    if "client" not in config:
        config.add_section("client")
        config.set("client", "anon_clientid", id)
    elif "anon_clientid" not in config["client"]:
        config.set("client", "anon_clientid", id)
    else:
        return config.get("client", "anon_clientid")
    with path.open("w") as f:
        config.write(f)
    return id


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
    #: A random id of the transfer session with time.
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
    transfer_stats: Optional[Dict] = None
    #: The collection of error message and the function responsible if the transfer fails.
    error_dict: Optional[Dict] = None
    #: The time of the log sent to Loki. It is None if not sent and stored locally in /tmp.
    sent_time_ms: Optional[int] = None


class UsageClient:
    """The client implementation for usage report.
    It is in charge of writing usage stats to the /tmp directory
    and report usage stats.
    """

    def __init__(self, client_id: Optional[str] = None):
        if client_id:
            self.client_id = client_id
        else:
            self.client_id = get_clientid()
        self.session_id = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"

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
        session_start_timestamp_ms: Optional[int] = None,
    ):
        if cls.enabled():
            client = cls()
            error_dict = {"loc": location, "message": str(exception)[:150]}
            stats = client.make_error(
                error_dict=error_dict,
                arguments_dict=args,
                src_region_tag=src_region_tag,
                dest_region_tag=dest_region_tag,
                session_start_timestamp_ms=session_start_timestamp_ms,
            )
            destination = client.write_usage_data(stats)
            client.report_usage_data("error", stats, destination)

    @classmethod
    def log_transfer(
        cls,
        transfer_stats: Optional[Dict],
        args: Optional[Dict] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
        session_start_timestamp_ms: Optional[int] = None,
    ):
        if cls.enabled():
            client = cls()
            stats = client.make_stat(
                arguments_dict=args,
                transfer_stats=transfer_stats,
                src_region_tag=src_region_tag,
                dest_region_tag=dest_region_tag,
                session_start_timestamp_ms=session_start_timestamp_ms,
            )
            destination = client.write_usage_data(stats)
            client.report_usage_data("usage", stats, destination)

    @classmethod
    def usage_stats_status(cls) -> UsageStatsStatus:
        # environment vairable has higher priority
        usage_stats_enabled_env_var = os.getenv(USAGE_STATS_ENABLED_ENV_VAR)
        if usage_stats_enabled_env_var is None:
            pass
        elif not _map_type(usage_stats_enabled_env_var, bool):
            return UsageStatsStatus.DISABLED_EXPLICITLY
        elif _map_type(usage_stats_enabled_env_var, bool):
            return UsageStatsStatus.ENABLED_EXPLICITLY
        elif usage_stats_enabled_env_var is not None:
            raise ValueError(
                f"Valid value for {USAGE_STATS_ENABLED_ENV_VAR} "
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

    @imports.inject("typer")
    @classmethod
    def set_usage_stats_via_config(typer, cls, value, config):
        current_status = cls.usage_stats_status()
        if current_status is UsageStatsStatus.DISABLED_EXPLICITLY:
            if (isinstance(value, bool) and not value) or (isinstance(value, str) and not _map_type(value, bool)):
                rprint("Usage stats collection is already disabled.")
                rprint(USAGE_STATS_REENABLE_MESSAGE)
                return
        elif current_status is UsageStatsStatus.ENABLED_EXPLICITLY:
            if (isinstance(value, bool) and value) or (isinstance(value, str) and _map_type(value, bool)):
                rprint("Usage stats collection is already enabled.")
                rprint(USAGE_STATS_REENABLED_MESSAGE)
                return

        if (isinstance(value, bool) and not value) or (isinstance(value, str) and not _map_type(value, bool)):
            prompt = "Would you still like to opt out of sharing anonymous usage metrics?"
            rprint(USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE + "\n")
            answer = typer.confirm(prompt, default=False)
            if not answer:
                # Do nothing if the user confirms not to disable
                return
            else:
                rprint(USAGE_STATS_REENABLE_MESSAGE)

        try:
            config.set_flag("usage_stats", value)
        except Exception as e:
            raise Exception("Failed to enable/disable by writing to" f"{config_path}") from e

        if config.get_flag("usage_stats"):
            rprint(USAGE_STATS_REENABLED_MESSAGE)

    def make_stat(
        self,
        arguments_dict: Optional[Dict] = None,
        transfer_stats: Optional[Dict] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
        session_start_timestamp_ms: Optional[int] = None,
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
            schema_version=SCHEMA_VERSION,
            client_id=self.client_id,
            session_id=self.session_id,
            source_region=src_region,
            destination_region=dest_region,
            source_cloud_provider=src_provider,
            destination_cloud_provider=dest_provider,
            os=sys.platform,
            session_start_timestamp_ms=session_start_timestamp_ms if session_start_timestamp_ms else int(time.time() * 1000),
            arguments_dict=arguments_dict,
            transfer_stats=transfer_stats,
        )

    def make_error(
        self,
        error_dict: Dict,
        arguments_dict: Optional[Dict] = None,
        src_region_tag: Optional[str] = None,
        dest_region_tag: Optional[str] = None,
        session_start_timestamp_ms: Optional[int] = None,
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
            schema_version=SCHEMA_VERSION,
            client_id=self.client_id,
            session_id=self.session_id,
            source_region=src_region,
            destination_region=dest_region,
            source_cloud_provider=src_provider,
            destination_cloud_provider=dest_provider,
            os=sys.platform,
            session_start_timestamp_ms=session_start_timestamp_ms if session_start_timestamp_ms else int(time.time() * 1000),
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
        destination = dir_path / USAGE_STATS_FILE

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

        prom_labels = {"type": type, "environment": "api"}
        headers = {"Content-type": "application/json"}
        data.sent_time_ms = int(time.time() * 1000)
        payload = {"streams": [{"stream": prom_labels, "values": [[str(_get_current_timestamp_ns()), json.dumps(asdict(data))]]}]}
        payload = json.dumps(payload)
        r = requests.post(LOKI_URL, headers=headers, data=payload, timeout=0.5)

        if r.status_code == 204:
            with open(path, "w") as json_file:
                json_file.write(json.dumps(asdict(data)))
