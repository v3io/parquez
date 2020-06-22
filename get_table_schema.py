from mlrun import get_or_create_ctx
from core.kv_table import KVTable
from config.app_conf import AppConf

CONFIG_PATH = 'User/parquez/config/parquez.ini'
REAL_TIME_TABLE_NAME = 'faker'


def main(context):
    context.logger.info("loading configuration")
    conf = AppConf(context.logger, CONFIG_PATH)

    kv_table = KVTable(context.logger, conf, REAL_TIME_TABLE_NAME)
    schema = kv_table.import_table_schema()

    context.logger.info("logging schema")
    context.log_artifact(schema)


if __name__ == '__main__':
    context = get_or_create_ctx('get-schema')
    main(context)