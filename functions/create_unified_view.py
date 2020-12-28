from mlrun import get_or_create_ctx
from config.app_conf import AppConf
from core.params import Params
from core.presto_client import PrestoClient
from core.kv_table import KVTable
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
    kv_table = KVTable(context.logger, conf, params)
    kv_table.import_table_schema()
    schema = kv_table.get_schema_fields()
    p_client = PrestoClient(context.logger, conf, params)
    uv = UnifiedView(context.logger, params, conf, schema, p_client)
    uv.execute_script_in_presto()


if __name__ == '__main__':
    ctx = get_or_create_ctx('generating presto view')
    main(ctx)