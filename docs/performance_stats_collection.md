# Performance Stats Collection

Skyplane collects performance statistics. Some of this data is shared with the Skyplane developers to improve the accuracy of the Skyplane solver and for research purposes.

Here are the guiding principles of our collection policy:

- **No surprises** — you will be notified before we begin collecting data. You will be notified of any changes to the data being collected or how it is used.
- **Easy opt-out:** You will be able to easily opt-out of data collection.
- **Transparency** — you will be able to review all data that is sent to us.
- **Control** — you will have control over your data, and we will honor requests to delete your data.
- We will **not** collect any personally identifiable data or proprietary code/data.
- We will **not** sell data or buy data about you.

## What data is collected

We collect non-sensitive data that helps us understand how Skyplane is used. **Personally identifiable data will never be collected.** Please check `/tmp/skyplane/usage/{client_id}/{session_id}/usage_stats.json` json file to see the data we collect. Explicitly, we collect:

1. System and OS information (OS version, kernel version, Python version).
2. Anonymized client id and transfer session id.
3. Source region and destination region per transfer.
4. The collection of command arguments used in the transfer session.
5. Total runtime and the aggregated transfer speed in Gbps.
6. Error message if the transfer fails.

## How to disable it

There are two ways to disable performance stats collection:

1. Run `skyplane config set usage_stats false` to disable collection for all future transfers. This won’t affect currently running transfers. Under the hood, this command writes `{"usage_stats": false}` to the global config file `~/.skyplane/config.json`.
2. Set the environment variable `SKYPLANE_USAGE_STATS_ENABLED` to 0, which temporarily disable the performance stats collection.

Currently there is no way to enable or disable collection for a running transfer; you have to stop and restart the transfer.

## How does it work

When Skyplane runs `skyplane cp` or `skyplane sync` command, it will decide whether performance stats collection should be enabled or not by considering the following factors in order:

1. It checks whether the environment variable `SKYPLANE_USAGE_STATS_ENABLED` is set: 1 means enabled and 0 means disabled.
2. If the environment variable is not set, it reads the value of key `usage_stats` in the global config file `~/.skyplane/config.json`: true means enabled and false means disabled. If there is no such key in global config file, then the performance stats collection is enabled by default.

Note: performance stats collection is first-time enabled by default when running `skyplane init`.

## Requesting removal of collected data

To request removal of collected data, please email us at admin@skyplane.org with the `client_id` that you can find in `/tmp/skyplane/usage/{client_id}/{session_id}/usage_stats.json`.

## Frequently Asked Questions (FAQ)

**Does the client_id and session_id map to personal data?**

No, the uuid will be a random ID that cannot be used to identify a specific person nor machine. It will not live beyond the lifetime of your Skyplane transfer session; and is primarily captured to enable us to honor deletion requests.

The `client_id` and `session_id` are logged so that deletion requests can be honored.

**Could an enterprise easily configure an additional endpoint or substitute a different endpoint?**

We definitely see this use case and would love to chat with you to make this work – email admin@skyplane.org.

## Contact us

If you have any feedback regarding performance stats collection, please email us at admin@skyplane.org.