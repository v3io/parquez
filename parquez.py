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
from core.input_parser import InputParser
from utils.logger import Logger
from core.parquet_table import ParquetTable
from core.cron_tab import Crontab
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.presto_client import PrestoClient
from utils.utils import Utils
import logging

CONFIG_PATH = 'config/parquez.ini'
LEVEL = logging.INFO


def main():
    logger = Logger(LEVEL)
    logger.info("Starting to Parquezzzzzzzz")

    parser = InputParser(logger)
    args = parser.parse_args()
    logger.info("input parsed")

    if args.config is not None:
        config_path = args.config
    else:
        config_path = CONFIG_PATH

    conf = AppConf(logger, config_path)
    logger.info("loading configuration")

    # should be deleted from 2.3 versions
    logger.info("initializing setup")
    utils = Utils(logger, conf)
    utils.copy_to_v3io("v3io-spark2-tools_2.11.jar")

    logger.info("validating kv table")
    kv_table = KVTable(logger, conf, args.real_time_table_name)
    kv_table.import_table_schema()

    logger.info("generating parquet table")
    parquet = ParquetTable(logger,conf, utils, args.partition_by, kv_table)
    parquet.generate_script()

    logger.info("generating view over kv")
    kv_view = KVView(logger, args.real_time_window, conf, kv_table)
    kv_view.generate_crete_view_script()

    logger.info("generating presto view")
    prest = PrestoClient(logger, conf, args.partition_by, parquet, kv_view, kv_table, args.view_name)
    prest.generate_unified_view()

    logger.info("generating cronJob")
    cr = Crontab(logger, conf, args.real_time_table_name, args.partition_interval, args.real_time_window,
                 args.historical_retention, args.partition_by)
    cr.create_cron_job()


if __name__ == '__main__':
    main()










