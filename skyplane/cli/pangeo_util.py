import netrc
import numpy as np
import os
import re
import requests
from rich.console import Console
from cmr import GranuleQuery
from typing import Tuple, List

console = Console()

def get_credentials() -> Tuple[str, str]:
    cred_file = "/Users/joe/.netrc"
    if not os.path.exists(cred_file):
        return None
    with open(cred_file) as f:
        cred = f.readline()
        if len(cred) == 0:
            return None
        username = re.search(r'machine urs\.earthdata\.nasa\.gov login (.*?) password', cred).group(1)
        password = cred.split('password')[1].strip()
    return username, password

def get_download_urls() -> List[str]:
    collection_shortname = ['GPM_3IMERGDL']

    # Get a list of granules for this collection from CMR
    # Each Granule is a file, provided to us as a HTTPS URL
    api_granule = GranuleQuery()
    api_granule.parameters(
        short_name=collection_shortname,
    )

    # retrieve all the granules
    granules = api_granule.get_all()

    # Find list of all downloadable URLs for the granules
    url_list = []
    # total_bytes = 0
    for i in range(0, np.shape(granules)[0]):
        for element in granules[i]['links']:
            if element['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/data#':
                # print('adding url: ' + element['href'])
                url = element['href']
                url_list.append(url)
                # total_bytes += int(requests.head(url).headers['Content-Length'])
                break
    console.print(f"{len(url_list)} url(s) will be downloaded.")
    return url_list
