from mlrun import get_or_create_ctx
from core.kv_table import KVTable
from config.app_conf import AppConf
from core.parquet_table import ParquetTable
from utils.utils import Utils
from core.k8s_client import K8SClient

CONFIG_PATH = 'User/parquez/config/parquez.ini'
REAL_TIME_TABLE_NAME = 'faker'
PARTITION_BY = 'h'



def main(context, partition_by):
    context.logger.info("loading configuration")
    conf = AppConf(context.logger, CONFIG_PATH)

    utils = Utils(context.logger, conf)

    kv_table = KVTable(context.logger, conf, REAL_TIME_TABLE_NAME)

    context.logger.info("generating parquet table")
    parquet = ParquetTable(context.logger, conf, utils, partition_by, kv_table, K8SClient(context.logger))
    parquet.generate_script()


if __name__ == '__main__':
    context = get_or_create_ctx('create-parquet-table')
    main(context,PARTITION_BY)