from logger import Logger
from parquettablegenerator import ParquetTableGenerator
from config.appconf import AppConf

# test_config.py


def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, config_path='../config/parquez.ini')
    parquet = ParquetTableGenerator(logger, 'kv_table_name', 'schema.txt', '1h', conf)
    parquet.generate_script()
