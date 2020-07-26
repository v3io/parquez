from mlrun import get_or_create_ctx
from core.kv_table import KVTable
from config.app_conf import AppConf

CONFIG_PATH = '/User/parquez/config/parquez.ini'
REAL_TIME_TABLE_NAME = 'faker'


def main(context, config_path=CONFIG_PATH):
    context.logger.info("loading configuration")
    p_config_path = context.parameters['config_path']
    if p_config_path:
        config_path = p_config_path
    conf = AppConf(context.logger, config_path)
    kv_table = KVTable(context.logger, conf, REAL_TIME_TABLE_NAME)
    kv_table.import_table_schema()
    parquet_schema = kv_table.get_schema_fields_and_types()
    context.logger.info("logging schema")
    context.log_artifact('parquet_schema', body=parquet_schema, local_path='parquet_schema.txt')


if __name__ == '__main__':
    context = get_or_create_ctx('get-schema')
    main(context)