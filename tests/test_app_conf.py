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
from utils.logger import Logger
from config.app_conf import AppConf


# test_app_conf.py
def test_app_conf():
    logger = Logger()
    cf = AppConf(logger, config_path='../config/parquez.ini')
    assert cf.v3io_container == "bigdata"
    assert cf.v3io_access_key== "<access_key>"
    assert cf.hive_schema == "default"
    assert cf.presto_uri == "<localhost>"
    assert cf.v3io_connector == "v3io"
    assert cf.hive_connector == "hive"
    assert cf.v3io_api_endpoint_host == "<localhost>"
    assert cf.v3io_api_endpoint_port == "8081"
    assert cf.username == "<user_name>"
    assert cf.password == "<password>"
