from mlrun import get_or_create_ctx
from core.kv_table import KVTable
from config.app_conf import AppConf
from core.params import Params


def main(context):
    context.logger.info("loading configuration")
    p_config_path = context.parameters['config_path']
    if p_config_path:
        config_path = p_config_path
    conf = AppConf(context.logger, config_path)
    params = Params()
    params.set_params_from_context(context)
    kv_table = KVTable(context.logger, conf, params)
    kv_table.import_table_schema()
    parquet_schema = kv_table.get_schema_fields_and_types()
    context.logger.info("logging schema")
    context.log_artifact('parquet_schema', body=parquet_schema, local_path='parquet_schema.txt')


if __name__ == '__main__':
    context = get_or_create_ctx('get-schema')
    main(context)
