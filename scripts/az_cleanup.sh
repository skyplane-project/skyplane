#!/bin/bash
set -xe
az group list | jq -c ".[].name" | xargs -L 1 echo | parallel --progress -j0 az group delete --yes --no-wait --name {}
echo "\n Queued cleanup of resource groups. Watching for completion..."
az group list | jq -c ".[].name" | xargs -L 1 echo | parallel --progress -j0 az group delete --yes --name {}