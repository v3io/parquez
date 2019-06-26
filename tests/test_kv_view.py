from utils.logger import Logger
from config.app_conf import AppConf
from utils.utils import Utils
# from core.parquet_table import ParquetTable
from core.kv_table import KVTable
from core.kv_view import KVView

KVTABLE_NAME = "booking_service_kv"
# test_kv_view.py


def test_kv_view():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    kv_table = KVTable(logger, conf, KVTABLE_NAME)
    kv_table.import_table_schema()
    # utils = Utils(logger, conf)
    # parquet = ParquetTable(logger, conf, utils, '1h', kv_table)
    # parquet.generate_script()
    kv_view = KVView(logger, '5m', conf, kv_table)
    kv_view.generate_crete_view_script()

