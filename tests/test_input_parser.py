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
from core.input_parser import InputParser
import pytest


# test_config.py


def test_generate():
    logger = Logger()
    ip = InputParser(logger)
    parser = ip.create_parser()
    args = parser.parse_args(['--view-name', 'parquezView'
                          ,'--partition-by', '1h'
                          ,'--partition-interval', '1h'
                          ,'--real-time-window', '3h'
                          ,'--historical-retention', '21h'
                          ,'--real-time-table-name', 'booking_service_kv'])
    print(args)


def test_no_args():
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        logger = Logger()
        ip = InputParser(logger)
        parser = ip.create_parser()
        parser.parse_args([])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 2
