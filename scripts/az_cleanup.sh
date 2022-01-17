#!/bin/bash

echo "If you want to filter this list to a subset, pass an argument as a prefix: az_cleanup.sh <prefix>"

AZURE_GROUPS=$(az group list | jq -c ".[].name")

# if argument is passed, filter output with grep
if [ -n "$1" ]; then
  AZURE_GROUPS=$(echo $AZURE_GROUPS | grep $1)
fi

# exit if no groups found
if [ -z "$AZURE_GROUPS" ]; then
  echo "No groups found"
  exit 0
fi

# confirm with user that they want to delete all resources
echo "This will delete all resources in all groups in your subscription."
for AZ_GROUP in $AZURE_GROUPS; do
  echo "    Group: $AZ_GROUP"
done

echo "Are you sure you want to delete all resources in all groups in your subscription?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) break;;
        No ) exit;;
    esac
done

set -xe
az group list | jq -c ".[].name" | xargs -L 1 echo | parallel --progress -j0 az group delete --yes --no-wait --name {}
echo "\n Queued cleanup of resource groups. Watching for completion..."
az group list | jq -c ".[].name" | xargs -L 1 echo | parallel --progress -j0 az group delete --yes --name {}
