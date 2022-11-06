"""Stream class for tap-bamboohr."""

import base64
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from functools import cached_property
import requests
import time
import json
import urllib.parse
import singer

LOGGER = singer.get_logger()

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class BamboohrApi(object):
    
    bamboohr_streams=["employees_directory", "timeoffs" ]

    def __init__(
        self,
        api_key_encrypted,
        subdomain,
        url_base= "https://api.bamboohr.com/api/gateway.php",
        retry=0,
    ):
        self.api_key_encrypted = api_key_encrypted,
        self.url_base = url_base,
        self.subdomain = subdomain,
        self.retry = retry


    def get_sync_endpoints(self, stream, subdomain, parameters={}):
        current_retry = 0
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {self.api_key_encrypted}",
        }

        if stream == "employees_directory":
            url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/employees/directory"
        elif stream == "timeoffs":
            url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/time_off/requests/?{urllib.parse.unquote(urllib.parse.urlencode(parameters))}"


        try:
            response = requests.get(url, headers=headers, timeout=60)

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
                if stream == "employees_directory":
                    records = [records["employees"]]
                elif stream == "timeoffs":
                    records = [records]
                for record in records:
                    LOGGER.info(f"the record to be sent -  {record}")
                    yield record
        
        except Exception as e:
                if current_retry < self.retry:
                    LOGGER.warning(
                        f"I need to sleep 60 s, Because last get call to {url} raised exception : {e}"
                    )
                    time.sleep(60)
                    current_retry = current_retry + 1
                else:
                    raise e


