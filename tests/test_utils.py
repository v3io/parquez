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
from utils.utils import Utils


# test_utils.py
def test_copy_v3io_file():
    logger = Logger()
    logger.info("logging......")
    conf = AppConf(logger, "test.ini")
    utils = Utils(logger, conf)
    utils.copy_to_v3io("../v3io-spark2-tools_2.11.jar")


def test_list_containers():
    logger = Logger()
    logger.info("logging......")
    conf = AppConf(logger, "test.ini")
    utils = Utils(logger, conf)
    utils.list_containers()




