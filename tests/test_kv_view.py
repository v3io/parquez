from utils.logger import Logger
from config.app_conf import AppConf
from core.kv_view import KVView
from core.params import Params
from core.presto_client import PrestoClient

PARAMS = Params(partition_by='h', real_time_table_name="faker", user_name='avia',
                access_key='051b75cc-019b-49da-83cc-c17495ed6d99')


def test_kv_view():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    presto_client = PrestoClient(logger, conf, params)
    kv_view = KVView(logger, params, conf, presto_client)
    kv_view.generate_crete_view_script()
