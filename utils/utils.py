# Copyright 2019 Iguazio
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
#
import os
import requests


def get_request_url(v3io_api_endpoint_host, v3io_api_endpoint_port):
    return 'https://{}:{}/{}'.format(v3io_api_endpoint_host, v3io_api_endpoint_port, "/api/containers/")


def get_request_headers(v3io_session_key):
    return {
        'Content-Type': 'application/json',
        'X-v3io-session-key': v3io_session_key
    }


def send_request(logger, url, headers):
    try:
        #auth = requests.auth.HTTPBasicAuth(username, password)
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        logger.debug(response.status_code)
        logger.debug(response.content)
        return response.content

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))


class Utils(object):
    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf

    def copy_to_v3io(self, file):
        command = 'curl https://' + self.conf.v3io_api_endpoint_host + '/' + self.conf.v3io_container + '/ -H \'x-v3io-session-key: ' + self.conf.v3io_access_key + '\' --insecure --upload-file ' + file
        self.logger.info('copy command %s ' % command)
        os.system(command)

    def list_containers(self):
        url = get_request_url(self.conf.v3io_api_endpoint_host,self.conf.v3io_api_endpoint_port)
        headers = get_request_headers(self.conf.v3io_access_key)
        req = send_request(self.logger, url, headers)
        self.logger.info(req)
