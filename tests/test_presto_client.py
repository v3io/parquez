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
from core.parquet_table import ParquetTable
from core.kv_table import KVTable
from core.kv_view import KVView
from core.presto_client import PrestoClient


KVTABLE_NAME = "booking_service_kv"
# test_kv_view.py

def test_presto():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    kv_table = KVTable(logger, conf, KVTABLE_NAME)
    kv_table.import_table_schema()
    utils = Utils(logger, conf)
    parquet = ParquetTable(logger, conf, utils, '1h', kv_table)
    #parquet.generate_script()
    kv_view = KVView(logger, '3h', conf, kv_table)
    kv_view.generate_crete_view_script()
    presto_cli= PrestoClient(logger,conf,'1h',parquet,kv_view,kv_table)
    presto_cli.generate_unified_view()
