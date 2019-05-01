from utils.logger import Logger
from utils.utils import Utils
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable


KVTABLE_NAME = "booking_service_kv"


# parquet_table_generator.py
def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    kv_table = KVTable(logger, conf, KVTABLE_NAME)
    kv_table.import_table_schema()
    utils = Utils(logger, conf)
    parquet = ParquetTable(logger, conf, utils, '1h', kv_table)
    parquet.generate_script()


