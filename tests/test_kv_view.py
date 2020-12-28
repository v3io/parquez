from utils.logger import Logger
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.params import Params

PARAMS = Params(partition_by='h',
                real_time_table_name="faker",
                user_name='avia',
                access_key='c8595589-097a-496d-8a46-e5dc3689ee37',
                )

def test_kv_view():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    kv_table = KVTable(logger, conf , params)
    kv_table.import_table_schema()
    parquet_schema = kv_table.get_schema_fields_and_types()
    kv_view = KVView(logger, params, conf)
    kv_view.generate_crete_view_script()


