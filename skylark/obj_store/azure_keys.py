"""
Azure account config
===
Performance: standard
subscription: skylark-azure-paras
redundancy: locally-redundant storage (LRS)
Routing Preferance: Microsoft Network Routing
"""
azure_storage_credentials = {
    "eastus": {
        "name": "skyeastus",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skyuseast;AccountKey=BgxPfOR5GB+0SR7B+qIgSly1Ih+M2xfOhtqxornE18N+2MyULBqH1QG7lmro/+o3UncwUuc8m4AduqFY3sC7UA==;EndpointSuffix=core.windows.net",
    },
    "westus": {
        "name": "skyuswest",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skyuswest;AccountKey=vvBaJdH8ndD65f5VGuUTUSiHzKse82+ogr+1qfaEAXFjtcgm7WAol78eHPNE4liSE/79QrfoMxz3MtgA0iIXNw==;EndpointSuffix=core.windows.net",
    },
    "centralus": {
        "name": "skycentralus",
        "connection_string": "DefaultEndpointsProtocol=https;AccountName=skycentralus;AccountKey=xSIiDWw10JZHuS6reLJdzmebxBwctRwpd/hNOJ4C/ciKvy2ez57oRN7ZF5A3ETY495A2wcO+Lutf0feyEdWU2A==;EndpointSuffix=core.windows.net",
    },
}
