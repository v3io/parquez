from mlrun import get_or_create_ctx
from pyspark.sql import SparkSession
from os import path


def generate_kv_parquet_path(container='parquez',
                             table='faker',
                             compress_type='parquet',
                             partition_by='h',
                             real_time_window='0h'):
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


def main(ctx):
    paths = generate_kv_parquet_path()
    ctx.logger.info("paths : {}".format(paths))
    if path.isdir(paths['fuse_kv_path']):
        coalesce = 6
        # Initiate a Spark Session
        spark = SparkSession.builder.appName("Spark JDBC to Databases - ipynb").getOrCreate()

        df = spark.read.format('io.iguaz.v3io.spark.sql.kv').load(paths['kv_path'])
        df.show()
        df.repartition(coalesce).write.mode('overwrite').parquet(paths['parquet_path'])
    else:
        print("Directory {} Doesnt exist".format(paths['fuse_kv_path']))


if __name__ == '__main__':
    context = get_or_create_ctx('run kv to parquet')
    main(context)
