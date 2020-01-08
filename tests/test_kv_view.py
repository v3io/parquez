from utils.logger import Logger
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView

KVTABLE_NAME = "fakerhourly"
# test_kv_view.py


def test_kv_view():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    kv_table = KVTable(logger, conf, KVTABLE_NAME)
    kv_table.import_table_schema()
    kv_view = KVView(logger, '1h', conf, kv_table)
    kv_view.generate_crete_view_script()

