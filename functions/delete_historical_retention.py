from mlrun import get_or_create_ctx
from config.app_conf import AppConf
from core.parquet_table import ParquetTable
from core.presto_client import PrestoClient
from core.params import Params


def get_bytes_from_file(filename):
    with open(filename, "r") as f:
        output = f.read()
    return output


def main(context):
    context.logger.info("starting parquet add partition job")
    p_config_path = context.parameters['config_path']
    if p_config_path:
        config_path = p_config_path
    conf = AppConf(context.logger, config_path)
    params = Params()
    params.set_params_from_context(context)
    context.logger.info(params.__dict__)
    historical_path = context.parameters['historical_path']
    context.logger.info("generating parquet table")
    p_client = PrestoClient(context.logger, conf, params)
    parquet = ParquetTable(context.logger, conf, params, p_client)
    parquet.drop_partition_from_path(historical_path)


if __name__ == '__main__':
    ctx = get_or_create_ctx('create-parquet-table')
    main(ctx)
