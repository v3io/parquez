from utils.logger import Logger
from utils.utils import Utils
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable


KVTABLE_NAME = "booking_service_kv"
TARGET = "v3io://parquez/faker_Parquet/year=2020/month=07/day=23/hour=05"
SOURCE = "v3io://parquez/faker/year=2020/month=07/day=23/hour=14"


def test_parquet_table_generator():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    kv_table = KVTable(logger, conf, KVTABLE_NAME)
    kv_table.import_table_schema()
    utils = Utils(logger, conf)
    parquet = ParquetTable(logger, conf, utils, 'h', kv_table)
    parquet.generate_script()


def test_parquet_table_check_partition():
    from core.delete_partitions import delete_partition
    delete_partition(SOURCE, TARGET)



