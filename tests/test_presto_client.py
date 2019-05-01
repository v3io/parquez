from utils.logger import Logger
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.presto_client import PrestoClient
from core.kv_table import KVTable

# test_config.py


def test_presto():
    logger = Logger()
    conf = AppConf(logger, config_path='test.ini')
    kvtable = KVTable('parquez', 'booking_service_kv', logger)
    parquet = ParquetTable(logger, '1h', conf, kvtable)
    #parquet.generate_script()
    prest = PrestoClient(logger,conf)
    #prest = Presto(logger, 'view_name', '1h',conf,kvtable)
    prest.execute_command()
