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
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.cron_tab import Crontab
from core.presto_client import PrestoClient

# test_config.py


def test_crontab():
    logger = Logger()
    conf = AppConf(logger, config_path='../config/parquez.ini')
    parquet = ParquetTable(logger, 'kv_table_name', 'schema.txt', '1h', conf)

    parquet.generate_script()
    prest = PrestoClient(logger, 'view_name', '1h', 'schema.txt', conf.presto_v3io_prefix(),'kv_table_name',
                   conf.presto_hive_prefix(), parquet.parquet_table_name, conf)
    prest.execute_command()
    cr = Crontab(logger, conf, 'kv_table_name','1h', '3h',
                 '7h', '1h')
    cr.create_cron_job()
