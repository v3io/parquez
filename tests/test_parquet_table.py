from utils.logger import Logger
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.k8s_client import K8SClient
from core.params import Params


# parquet_table_generator.py
def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = Params()
    params.partition_by ='h'
    params.real_time_table_name = "faker"
    kv_table = KVTable(logger, conf, params.real_time_table_name)
    kv_table.import_table_schema()
    parquet_schema = kv_table.get_schema_fields_and_types()
    parquet = ParquetTable(logger, conf, params, parquet_schema, K8SClient(logger))
    parquet.create()
    parquet.drop()


