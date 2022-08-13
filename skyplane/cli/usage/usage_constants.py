USAGE_STATS_ENABLED_ENV_VAR = "SKYPLANE_USAGE_STATS_ENABLED"

USAGE_STATS_FILE = "usage_stats.json"

USAGE_STATS_ENABLED_MESSAGE = (
    "Usage stats collection is enabled. " "To disable this, run `skyplane config set usage_stats false`. " "See xxx for more details. "
)

USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE = (
    "Usage statistics anonymous feedback is enabled by default. "
    "To disable this, add `--disable-global` "
    "to the command `skyplane metrics`. "
    "See xxx for more details. "
)

USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE = (
    "We are an academic research group working to improve inter-cloud network performance. "
    "We collect high-level performance data to improve the accuracy of our solver. "
    "Skyplane collects the following anonymous data: "
    "System and OS information (OS version, kernel version, Python version) "
    "Per transfer: "
    "Source region and destination region "
    "Total GB sent in anonymized ranges (i.e. 0-100MB, 100MB-1GB, 1GB-10GB, etc.) "
    "TODO "
    "You can inspect what we share in the /tmp/skyplane/metrics directory. "
    "You help us make your transfers faster by sharing with us better data about the throughput between cloud regions. "
)

USAGE_STATS_DISABLED_MESSAGE = "Usage stats collection is disabled."
