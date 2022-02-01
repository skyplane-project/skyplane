"""
Azure account config
===
Performance: standard
subscription: skylark-azure-paras
redundancy: locally-redundant storage (LRS)
Routing Preferance: Microsoft Network Routing
"""
azure_storage_credentials = {
    "eastusnonpremium": {
        "name": "skyeastus",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skyeastus;AccountKey=fHkWsVpW5LCdiOJEtPLfWcx1qvb3aXVu1+19RfQi7nndaWwek0ZvvMFtYD9eZe2i/3j0+gEIuvu4lC2OMmIeZQ==;EndpointSuffix=core.windows.net",
    },
    "westus": {
        "name": "skyuswest",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skywestus;AccountKey=WJtmHYayszZMHyzl2OQPyAG+VHlfh0n7RDEimZ4cDbuvT6SlrwcbbvC/zf6EvXF0dc0+DV8KABtXDj/Ro83qYg==;EndpointSuffix=core.windows.net",
    },
    "centralus": {
        "name": "skycentralus",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skycentralus;AccountKey=CRybVbNnJ0HFq6qZP7/llBXOls5X+vLZR5DL/Za9Taxj24urkfrSGrVy55tZne5zAXg/MWbZ1N+YC5RWWjfUiA==;EndpointSuffix=core.windows.net",
    },
    "eastus": {
        "name": "skypremiumblockuseast",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skypremiumblockuseast;AccountKey=lCJszpbQ33Q49geu+tVNO7nqKFtQMdrSOmmk9rmLcLSWB5Tt9CSsl5JLUXrEoHz9/gYlvBDUdrB8BN3FWwK7xQ==;EndpointSuffix=core.windows.net",
    },
    "skypremiumfileuseast": {
        "name": "skypremiumfileuseast",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skypremiumfileuseast;AccountKey=dFEPqWjniwzunO5TkvW5Y4Ds8YK3OvWEtrT2HIH3UTPC+oETPcZufBHoZyhJkfupwHd0247rKyFUDZsj7RI62g==;EndpointSuffix=core.windows.net",
    },
    "skypremiumpageuseast": {
        "name": "skypremiumpageuseast",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skypremiumpageuseast;AccountKey=pWuLQGXSAmM358jRMi94rcPAu0NQSYDSjk0+vl7fyNEAXpvdb3bclimBnDxpb2lsBfTrwTa7diWjSz62THbsMg==;EndpointSuffix=core.windows.net",
    },
}
