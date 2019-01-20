from core.logger import Logger
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable

# parquet_table_generator.py


def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, config_path='test_config.ini')
    kv_table = KVTable(conf, 'test', logger)
    kv_table.import_table_schema()
    parquet = ParquetTable(logger,'1h', conf, kv_table)
    parquet.generate_script()

