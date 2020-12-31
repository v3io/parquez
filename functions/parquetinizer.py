from mlrun import get_or_create_ctx, import_function
from core.params import Params
from config.app_conf import AppConf


def generate_kv_parquet_path(container='parquez',
                             table='faker',
                             compress_type='parquet',
                             partition_by='h',
                             real_time_window='3h'):
    real_time_window_delta = int(real_time_window[:-1])
    print(real_time_window_delta)
    from datetime import datetime, timezone, timedelta
    from dateutil.relativedelta import relativedelta
    current_date_path = None
    if partition_by == 'h':
        current_date_path = (datetime.now(timezone.utc) - timedelta(hours=real_time_window_delta)).strftime(
            "year=%Y/month=%m/day=%d/hour=%H")
    elif partition_by == 'd':
        current_date_path = (datetime.now(timezone.utc) - timedelta(days=real_time_window_delta)).strftime(
            "year=%Y/month=%m/day=%d")
    elif partition_by == 'm':
        current_date_path = (datetime.now(timezone.utc) - relativedelta(months=real_time_window_delta)).strftime(
            "year=%Y/month=%m")
    elif partition_by == 'y':
        current_date_path = (datetime.now(timezone.utc) - relativedelta(years=real_time_window_delta)).strftime(
            "year=%Y")
    kv_path = "v3io://{}/{}/{}/".format(container, table, current_date_path)
    parquet_path = "v3io://{}/{}_{}/{}/".format(container, table, compress_type, current_date_path)
    fuse_kv_path = "/v3io/{}/{}/{}/".format(container, table, current_date_path)
    return {'kv_path': kv_path, 'parquet_path': parquet_path, 'fuse_kv_path': fuse_kv_path}


def main(context):
    params = Params()
    params.set_params_from_context(context)
    #params_dic = params.__dict__
    context.logger.info("loading configuration")
    p_config_path = context.parameters['config_path']
    if p_config_path:
        config_path = p_config_path
    conf = AppConf(context.logger, config_path)
    path = generate_kv_parquet_path(conf.v3io_container,
                                    params.real_time_table_name,
                                    conf.compression,
                                    params.partition_by,
                                    params.real_time_window)
    fn = import_function(url="db://parquez/kv_to_parquet:latest")
    fn.spec.artifact_path = 'User/artifacts'
    fn.spec.service_account = 'mlrun-api'
    fn.run(params={'kv_path': path['kv_path'], 'fuse_kv_path': path['fuse_kv_path']}, artifact_path='/User/artifacts')
    fn = import_function(url="db://parquez/delete_kv_partition:latest")
    fn.spec.artifact_path = 'User/artifacts'
    fn.spec.service_account = 'mlrun-api'
    fn.run(params={'parquet_path': path['parquet_path']}, artifact_path='/User/artifacts')


if __name__ == '__main__':
    ctx = get_or_create_ctx('run kv to parquet')
    main(ctx)
