SCHEMA_VERSION = "0.1"
USAGE_STATS_ENABLED_ENV_VAR = "SKYPLANE_USAGE_STATS_ENABLED"
USAGE_STATS_FILE = "usage_stats.json"
USAGE_STATS_ENABLED_MESSAGE = (
    "[yellow]We collect high-level usage statistics for Skyplane. All data is anonymous and\n"
    "helps us maintain an open-source map of cloud network performance data. This data is used\n"
    "to improve the speed of your transfers by routing your traffic around slow links.[/yellow]\n"
    "To disable anonymous usage statistics, run [bold]skyplane config set usage_stats false[/bold]"
)
USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE = (
    "[green][bold]We are an academic research group working to improve inter-cloud network performance.[/bold] "
    "You can inspect what we share in the /tmp/skyplane/metrics directory. "
    "We do not collect any personal data and only collect high-level performance data to improve the accuracy of our solver.[/green]"
    "\n\nSkyplane collects the following anonymous data:"
    "\n    * System and OS information (OS version, kernel version, Python version)"
    "\n    * Source region and destination region per transfer"
    "\n    * Total GB sent in anonymized ranges (e.g. 1MB-100MB, 100MB-1GB, 1GB-10GB, etc.)"
    "\n    * Aggregated transfer speed in Gbps"
)
USAGE_STATS_REENABLE_MESSAGE = (
    "[yellow][bold]If you want to re-enable usage statistics, run `skyplane config set usage_stats true`.[/bold][/yellow]"
)
USAGE_STATS_REENABLED_MESSAGE = (
    "[green][bold]Thank you for your support of open-source research![/bold][/green]"
    "\nIf you want to disable usage statistics, run `skyplane config set usage_stats false`."
)
USAGE_STATS_DISABLED_MESSAGE = "Usage stats collection is disabled."