#!/bin/bash

echo "If you want to filter this list to a subset, pass an argument as a prefix: az_cleanup.sh <prefix>"


# if argument is passed, filter output with grep
if [ ! -z "$1" ]; then
  AZURE_GROUPS=$(az group list | jq -r -c ".[].name" | grep $1)
else
  AZURE_GROUPS=$(az group list | jq -r -c ".[].name")
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

for AZ_GROUP in $AZURE_GROUPS; do
  echo "Deleting resources in group: $AZ_GROUP"
  az group delete --yes --no-wait --name $AZ_GROUP
done
echo "\n Queued cleanup of resource groups. Watching for completion..."
for AZ_GROUP in $AZURE_GROUPS; do
  echo "Waiting for deletion of $AZ_GROUP"
  az group delete --yes --name $AZ_GROUP
done

