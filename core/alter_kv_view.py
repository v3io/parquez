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
import sys
from kv_table import KVTable
from kv_view import KVView


CONFIG_PATH = 'config/parquez.ini'


def parse_kv_window_arg(val, logger):
    val = val.replace('hours', 'h')
    val = val.replace('days', 'd')
    val = val.replace('months', 'M')
    val = val.replace(" ", "")
    logger.debug("parsed kv window val " + val)
    return val


def main():
    logger = Logger()
    logger.info("altering view")

    args = sys.argv

    conf = AppConf(logger, CONFIG_PATH)

    logger.info("validating kv table " + args[1])
    kv_table = KVTable(logger, conf, args[1])

    logger.info("generating view over kv" + args[2])
    parsed_window = parse_kv_window_arg(args[2], logger)
    kv_view = KVView(logger, parsed_window, conf, kv_table)
    kv_view.generate_crete_view_script()


if __name__ == '__main__' and __package__ is None:
    if __package__ is None:
        from os import path
        sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
        from config.app_conf import AppConf
        from utils.logger import Logger
    else:
        from ..config.app_conf import AppConf
    main()
