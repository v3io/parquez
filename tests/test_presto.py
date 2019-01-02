from logger import Logger
from parquettablegenerator import ParquetTableGenerator
from config.appconf import AppConf
from presto import Presto

# test_config.py


def test_presto():
    logger = Logger()
    conf = AppConf(logger, config_path='../config/parquez.ini')
    parquet = ParquetTableGenerator(logger, 'kv_table_name', 'schema.txt', '1h', conf)

    parquet.generate_script()
    prest = Presto(logger, 'view_name', '1h', 'schema.txt', conf.presto_v3io_prefix(),'booking_service_kv',
                   conf.presto_hive_prefix(), parquet.parquet_table_name, conf)
    prest.execute_command()
