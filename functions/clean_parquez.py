from mlrun import get_or_create_ctx
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.presto_client import PrestoClient
from core.params import Params
from core.unified_view import UnifiedView


def get_bytes_from_file(filename):
    with open(filename, "r") as f:
        output = f.read()
    return output


def main(context):
    context.logger.info("loading configuration")
    p_config_path = context.parameters['config_path']
    if p_config_path:
        config_path = p_config_path
    conf = AppConf(context.logger, config_path)
    params = Params()
    params.set_params_from_context(context)
    presto_client = PrestoClient(context.logger, conf, params)

    context.logger.info("validating kv table")
    kv_table = KVTable(context.logger, conf, params)
    kv_table.import_table_schema()
    schema = kv_table.get_schema_fields_and_types()
    parquet = ParquetTable(context.logger, conf, params, presto_client)
    parquet.drop()

    context.logger.info("generating view over kv")
    kv_view = KVView(context.logger, params, conf, presto_client)
    kv_view.drop_view()

    unified_view = UnifiedView(context.logger, params, conf, schema, presto_client)
    unified_view.drop_view()


if __name__ == '__main__':
    ctx = get_or_create_ctx('clean_parquez')
    main(ctx)










