"""Stream class for tap-bamboohr."""

import base64
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
import requests
import time
import json
LOGGER = singer.get_logger()

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class BamboohrApi(object):
    def __init__(
        self,
        api_key,
        # subdomain,
        retry=10,
    ):
        self.api_key = api_key
        self.retry = retry

    def get_employees(self):

        url = "https://api.bamboohr.com/api/gateway.php/sendinblue/v1/employees/directory"

        headers = {
            "accept": "application/json",
            "authorization": f"Basic {self.api_key}",
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            if current_retry < self.retry:
                LOGGER.warning(
                    f"Unexpected response status_code {response.status_code} i need to sleep 60s before retry {current_retry}/{self.retry}"
                )
                time.sleep(60)
                current_retry = current_retry + 1
            else:
                raise RuntimeError(
                    f"Too many retry, last response status_code {response.status_code} : {response.content}"
                )
        else:
            records = json.loads(response.content.decode("utf-8"))
            if isinstance(records, dict):
                LOGGER.debug("Last call returned one document, convert it to list of one document")
                records = [records]
            return records

# class TapBambooHRStream(RESTStream):
#     """BambooHR stream class."""

#     _LOG_REQUEST_METRIC_URLS: bool = True
#     @property
#     def url_base(self) -> str:
#         subdomain = self.config.get("subdomain")
#         return f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1"

#     @property
#     def http_headers(self) -> dict:
#         """Return the http headers needed."""
#         headers = {}
#         if "user_agent" in self.config:
#             headers["User-Agent"] = self.config.get("user_agent")
#         headers["Content-Type"] = "application/json"
#         headers["Accept"] = "application/json"
#         return headers

#     @property
#     def authenticator(self):
#         http_headers = {}
#         auth_token = self.config.get("auth_token")
#         basic_auth = f"{auth_token}:nothingtoseehere"
#         http_headers["Authorization"] = "Basic " + base64.b64encode(
#             basic_auth.encode("utf-8")
#         ).decode("utf-8")
#         return SimpleAuthenticator(stream=self, auth_headers=http_headers)

# class Employees(TapBambooHRStream):
#     name = "employees"
#     path = "/employees/directory"
#     primary_keys = ["id"]
#     records_jsonpath = "$.employees[*]"
#     replication_key = None
#     schema_filepath = SCHEMAS_DIR / "directory.json"

