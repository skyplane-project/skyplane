# (C) Copyright Samsung SDS. 2023

#

# Licensed under the Apache License, Version 2.0 (the "License");

# you may not use this file except in compliance with the License.

# You may obtain a copy of the License at

#

#     http://www.apache.org/licenses/LICENSE-2.0

#

# Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.
""" SCP Open-API utility functions """

import base64
import datetime
from functools import wraps
import hashlib
import hmac
import os
import json
import random
import time
from urllib import parse
import requests
from skyplane.utils import logger

CREDENTIALS_PATH = "~/.scp/scp_credential"
API_ENDPOINT = "https://openapi.samsungsdscloud.com"
TEMP_VM_JSON_PATH = "/tmp/json/tmp_vm_body.json"


class SCPClientError(Exception):
    pass


class SCPOngoingRequestError(Exception):
    pass


class SCPCreationFailError(Exception):
    pass


def raise_scp_error(response: requests.Response) -> None:
    """Raise SCPCloudError if appropriate."""
    status_code = response.status_code
    if status_code == 200 or status_code == 202:
        return
    try:
        resp_json = response.json()
        message = resp_json["message"]
    except (KeyError, json.decoder.JSONDecodeError):
        # print(f"response: {response.content}")
        raise SCPClientError(f"Unexpected error. Status code: {status_code}")

    raise SCPClientError(f"{status_code}: {message}")


def _retry(method, max_tries=60, backoff_s=1):
    @wraps(method)
    def method_with_retries(self, *args, **kwargs):
        try_count = 0
        while try_count < max_tries:
            try:
                return method(self, *args, **kwargs)
            except Exception as e:
                # print(e.args[0])
                retry_codes = ["500", "412", "403"]  # add 403 for object storage
                # if any(code in e.args[0] for code in retry_codes):
                if any(code in str(e) for code in retry_codes):
                    try_count += 1
                    # console.print(f"[yellow] retries: {method.__name__} - {e}, try_count : {try_count}[/yellow]")
                    logger.fs.debug(f"retries: {method.__name__} - {e}, try_count : {try_count}")
                    if try_count < max_tries:
                        time.sleep(backoff_s)
                    else:
                        raise e
                elif "Connection aborted" in str(e):
                    try_count += 1
                    # with open(f"/skyplane/retry_nocode_error.txt", "w") as f:
                    #     f.write(str(e))
                    logger.fs.debug(f"retries: {method.__name__} - {e}, try_count : {try_count}")
                    if try_count < max_tries:
                        time.sleep(backoff_s)
                    else:
                        raise e
                else:
                    # with open(f"/skyplane/retry_nocode_error.txt", "w") as f:
                    #     f.write(str(e))
                    raise e

    return method_with_retries


class SCPClient:
    """SCP Open-API client"""

    def __init__(self) -> None:
        # print('SCPClient init')
        self.credentials = os.path.expanduser(CREDENTIALS_PATH)
        if not os.path.exists(self.credentials):
            self.credentials = os.path.expanduser("/pkg/data/scp_credential")
        assert os.path.exists(self.credentials), "SCP Credentials not found"
        with open(self.credentials, "r") as f:
            lines = [line.strip() for line in f.readlines() if " = " in line]
            self._credentials = {line.split(" = ")[0]: line.split(" = ")[1] for line in lines}

        self.access_key = self._credentials["scp_access_key"]
        self.secret_key = self._credentials["scp_secret_key"]
        self.project_id = self._credentials["scp_project_id"]
        self.client_type = "OpenApi"
        self.timestamp = ""
        self.signature = ""

        self.headers = {
            "X-Cmp-AccessKey": f"{self.access_key}",
            "X-Cmp-ClientType": f"{self.client_type}",
            "X-Cmp-ProjectId": f"{self.project_id}",
            "X-Cmp-Timestamp": f"{self.timestamp}",
            "X-Cmp-Signature": f"{self.signature}",
        }

    @_retry
    def _get(self, url, contents_key="contents"):
        method = "GET"
        url = f"{API_ENDPOINT}{url}"
        self.set_timestamp()
        self.set_signature(url=url, method=method)

        response = requests.get(url, headers=self.headers)
        raise_scp_error(response)

        if contents_key is not None:
            return response.json().get(contents_key, [])
        else:
            return response.json()

    @_retry
    def _getDetail(self, url):
        method = "GET"
        url = f"{API_ENDPOINT}{url}"
        self.set_timestamp()
        self.set_signature(url=url, method=method)

        response = requests.get(url, headers=self.headers)
        response_detail = response.content.decode("utf-8")
        raise_scp_error(response)
        return json.loads(response_detail)

    def post(self, url, req_body):
        method = "POST"
        url = f"{API_ENDPOINT}{url}"
        self.set_timestamp()
        self.set_signature(url=url, method=method)

        response = requests.post(url, json=req_body, headers=self.headers)
        raise_scp_error(response)
        return response.json()

    @_retry
    def _post(self, url, req_body):
        return self.post(url, req_body)

    @_retry
    def _delete(self, url, req_body=None):
        method = "DELETE"
        url = f"{API_ENDPOINT}{url}"
        self.set_timestamp()
        self.set_signature(url=url, method=method)

        # try:
        if req_body:
            response = requests.delete(url, json=req_body, headers=self.headers)
        else:
            response = requests.delete(url, headers=self.headers)
        raise_scp_error(response)
        try:
            return response.json()
        except json.JSONDecodeError as e:
            # return response.content.decode('utf-8')
            return response

    def wait_for_completion(self, task_name, completion_condition, retry_limit=5, timeout=180):
        start = time.time()
        # console.print(f"[bright_black] Waiting for... {task_name} [/bright_black]")
        logger.fs.debug(f"Waiting for... {task_name}")

        retries = 0
        while time.time() - start < timeout and retries < retry_limit:
            try:
                if completion_condition():
                    break
            except Exception as e:
                retries += 1
                # print(f"Exception occurred during completion_condition(): {task_name} - {e}, retries : {retries}")
                logger.fs.debug(f"Exception occurred during completion_condition(): {task_name} - {e}, retries : {retries}")
            time.sleep(1)
        if retries == retry_limit:
            # console.print(f"[red] {task_name}... Retry limit reached [/red]")
            logger.fs.error(f"{task_name}... Retry limit reached")
            raise SCPCreationFailError(f"Failed to create {task_name}")
        else:
            # console.print(f"[green] {task_name}... Completed [/green]")
            logger.fs.debug(f"{task_name}... Completed")

    def set_timestamp(self) -> None:
        # self.timestamp = str(int(round((datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp() * 1000)))
        current_time = datetime.datetime.now()
        self.timestamp = str(int(round(current_time.timestamp() * 1000)))
        self.headers["X-Cmp-Timestamp"] = self.timestamp

    def set_signature(self, url: str, method: str) -> None:
        self.signature = self.get_signature(url=url, method=method)
        self.headers["X-Cmp-Signature"] = f"{self.signature}"

    def get_signature(self, url: str, method: str) -> str:
        url_info = parse.urlsplit(url)
        url = f"{url_info.scheme}://{url_info.netloc}{parse.quote(url_info.path)}"

        if url_info.query:
            enc_params = [(item[0], parse.quote(item[1][0])) for item in parse.parse_qs(url_info.query).items()]
            url = f"{url}?{parse.urlencode(enc_params)}"

        message = method + url + self.timestamp + self.access_key + self.project_id + self.client_type
        message = message.encode("utf-8")
        secret = self.secret_key.encode("utf-8")

        signature = base64.b64encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest()).decode("utf-8")

        return signature

    def random_wait(self):
        random_int = random.randint(1, 100)
        time.sleep(1 * random_int / 100)
