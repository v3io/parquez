from core.logger import Logger
from config.appconf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView

# test_kv_view.py


def test_kv_view():
    logger = Logger()
    conf = AppConf(logger, config_path='test_config.ini')
    kv_table = KVTable(conf, 'booking_service_kv', logger)
    kv_view = KVView(logger, '3h', conf, kv_table)
    kv_view.generate_crete_view_script()

