from mlrun import get_or_create_ctx
from config.app_conf import AppConf
from core.params import Params
from core.kv_view import KVView

CONFIG_PATH = '/User/parquez/config/parquez.ini'
REAL_TIME_TABLE_NAME = 'faker'

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
    context.logger.info("generating view over kv")
    kv_view = KVView(context.logger, params, conf)
    kv_view.generate_crete_view_script()


if __name__ == '__main__':
    context = get_or_create_ctx('create-parquet-table')
    main(context)