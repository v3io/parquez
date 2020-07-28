from mlrun import get_or_create_ctx
from config.app_conf import AppConf
from core.params import Params
from core.presto_client import PrestoClient
from core.kv_table import KVTable
from core.parquet_table import ParquetTable
from core.k8s_client import K8SClient
from core.kv_view import KVView


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
    kv_table = KVTable(context.logger, conf, params.real_time_table_name)
    kv_table.import_table_schema()
    schema = get_bytes_from_file(context.artifact_path + "/parquet_schema.txt")
    parquet = ParquetTable(context.logger, conf, params, schema, K8SClient(context.logger))
    context.logger.info("generating presto view")
    kv_view = KVView(context.logger, params, conf)
    prest = PrestoClient(context.logger, conf, params, parquet, kv_view, kv_table)
    prest.generate_unified_view()


if __name__ == '__main__':
    context = get_or_create_ctx('generating presto view')
    main(context)