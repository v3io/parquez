from logger import Logger
from parquettable import ParquetTable
from config.appconf import AppConf
from presto import Presto
from kvtable import KVTable

# test_config.py


def test_presto():
    logger = Logger()
    conf = AppConf(logger, config_path='../config/parquez.ini')
    kvtable = KVTable('parquez', 'booking_service_kv', logger)
    parquet = ParquetTable(logger, '1h', conf, kvtable)
    parquet.generate_script()
    prest = Presto(logger, 'view_name', '1h',conf,kvtable)
    prest.execute_command()
