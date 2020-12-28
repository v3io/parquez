from mlrun import get_or_create_ctx
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.presto_client import PrestoClient
from core.params import Params


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

    context.logger.info("validating kv table")
    kv_table = KVTable(context.logger, conf, params.real_time_table_name)

    context.logger.info("generating parquet table")
    schema = get_bytes_from_file(context.artifact_path+"/parquet_schema.txt")
    parquet = ParquetTable(context.logger, conf, params, schema, K8SClient(context.logger))
    parquet.drop()

    context.logger.info("generating view over kv")
    kv_view = KVView(context.logger, params, conf)
    kv_view.drop_view()

    context.logger.info("generating presto view")
    prest = PrestoClient(context.logger, conf, params, parquet, kv_view, kv_table)
    prest.drop_unified_view()


if __name__ == '__main__':
    context = get_or_create_ctx('parquez')
    main(context)










