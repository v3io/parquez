from utils.logger import Logger
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable


# parquet_table_generator.py


def test_parquet_table_generator():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    conf = AppConf(logger, "test.ini")
    conf.log_conf()

    logger.info("validating kv table")


    kv_table = KVTable(conf, "booking_service_kv", logger)
    kv_table.import_table_schema()
    parquet = ParquetTable(logger,'1h', conf, kv_table)

    parquet.generate_script()


