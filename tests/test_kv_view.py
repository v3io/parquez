from core.logger import Logger
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.parquet_table import ParquetTable

# test_kv_view.py


def test_kv_view():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    conf = AppConf(logger, "test.ini")
    conf.log_conf()

    logger.info("validating kv table")

    kv_table = KVTable(conf, "booking_service_kv", logger)
    kv_table.import_table_schema()
    #parquet = ParquetTable(logger, '1h', conf, kv_table)

    #parquet.generate_script()

    kv_view = KVView(logger, '3h', conf, kv_table)
    kv_view.generate_crete_view_script()

