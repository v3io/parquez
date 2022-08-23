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
import configparser
from utils.logger import Logger


class AppConf(object):
    def __init__(self, logger=Logger(), config_path='config/parquez.ini'):
        self.logger = logger
        config = configparser.ConfigParser()
        config.read(config_path)
        self.v3io_container = config['v3io']['v3io_container']
        self.v3io_access_key = config['v3io']['access_key']
        self.hive_schema = config['hive']['hive_schema']
        self.v3io_connector = config['presto']['v3io_connector']
        self.hive_connector = config['presto']['hive_connector']
        self.presto_uri = config['presto']['uri']
        self.v3io_api_endpoint_host = config['nginx']['v3io_api_endpoint_host']
        self.v3io_api_endpoint_port = config['nginx']['v3io_api_endpoint_port']
        self.username = config['nginx']['username']
        self.password = config['nginx']['password']
        self.compression = config['compression']['type']
        self.coalesce = config['compression']['coalesce']
        self.environment = config['environment']['type']

    def presto_v3io_prefix(self):
        return self.v3io_connector+"."+self.v3io_container

    def presto_hive_prefix(self):
        return self.hive_connector+"."+self.hive_schema







