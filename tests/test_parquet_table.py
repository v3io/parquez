from utils.logger import Logger
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.params import Params
from core.parquet_table import create_delete_external_table
from core.presto_client import PrestoClient

PARAMS = Params(partition_by='h', real_time_table_name="faker", user_name='avia',
                access_key='950573f3-1ff5-4b7e-892c-6255d284232c')


# parquet_table_generator.py
def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    presto_client = PrestoClient(logger, conf, params)
    parquet = ParquetTable(logger, conf, params, presto_client)
    parquet.create()
    parquet.drop()


def test_create_external_table_path():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    create_delete_external_table(logger, conf, params, 'PUT')


def test_create_table():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    presto_client = PrestoClient(logger, conf, params)
    create_delete_external_table(logger, conf, params, 'DELETE')
    parquet = ParquetTable(logger, conf, params, presto_client)
    parquet.create()


def test_add_partition():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    presto_client = PrestoClient(logger, conf, params)
    parquet = ParquetTable(logger, conf, params, presto_client)
    parquet.add_partition(year=2020, month=11, day=23, hour=14)


def test_drop_partition():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    presto_client = PrestoClient(logger, conf, params)
    parquet = ParquetTable(logger, conf, params, presto_client)
    parquet.drop_partition(year=2020, month=11, day=23, hour=14)


def test_split_path():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    params = PARAMS
    presto_client = PrestoClient(logger, conf, params)
    parquet = ParquetTable(logger, conf, params, presto_client)
    parquet.split_parquet_path_to_values('v3io://parquez/faker_Parquet/year=2021/month=01/day=03/hour=01/')
