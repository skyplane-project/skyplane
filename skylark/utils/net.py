import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def retry_requests(connect=3, backoff_factor=0.1):
    session = requests.Session()
    retry = Retry(connect=connect, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
