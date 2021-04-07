from utils.logger import Logger
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.unified_view import UnifiedView
from core.params import Params
from core.presto_client import PrestoClient

PARAMS = Params(partition_by='h', real_time_table_name="faker", user_name='avia',
                access_key='c8595589-097a-496d-8a46-e5dc3689ee37')


def test_unified_view():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    kv_table = KVTable(logger, conf, params)
    kv_table.import_table_schema()
    parquet_schema = kv_table.get_schema_fields()
    presto_client = PrestoClient(logger, conf, params)
    unified_view = UnifiedView(logger, params, conf, parquet_schema, presto_client)
    unified_view.execute_script_in_presto()


