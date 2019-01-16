from logger import Logger
from parquettable import ParquetTable
from config.appconf import AppConf
from kvtable import KVTable

# test_config.py


def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, config_path='../config/parquez.ini')
    kvtable = KVTable('parquez', 'booking_service_kv', logger)
    parquet = ParquetTable(logger,'1h', conf, kvtable)
    parquet.generate_script()

