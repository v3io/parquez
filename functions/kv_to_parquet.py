#!/usr/local/bin/python

# Locate v3iod:
from subprocess import run

run(["/bin/bash", "/etc/config/v3io/v3io-spark-operator.sh"])


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
    kv_path = "v3io://{}/{}/{}".format(container, table, current_date_path)
    parquet_path = "v3io://{}/{}_{}/{}".format(container, table, compress_type, current_date_path)
    print("kv path: {} , parquet_path : {} ".format(kv_path, parquet_path))
    return {'kv_path': kv_path, 'parquet_path': parquet_path}


coalesce = 6
# The pyspark code:
import os
from pyspark.sql import SparkSession
import os.path
from os import path


paths = generate_kv_parquet_path()
if path.isdir(paths['kv_path']):
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages mysql:mysql-connector-java:5.1.39 pyspark-shell"
    spark = SparkSession.builder.appName("Spark JDBC to Databases - ipynb").config("spark.driver.extraClassPath",
                                                                                   "/v3io/users/admin/mysql-connector"
                                                                                   "-java-5.1.45.jar").config(
        "spark.executor.extraClassPath", "/v3io/users/admin/mysql-connector-java-5.1.45.jar").getOrCreate()

    df = spark.read.format('io.iguaz.v3io.spark.sql.kv').load(paths['kv_path'])
    df.show()
    df.repartition(coalesce).write.mode('overwrite').parquet(paths['parquet_path'])
else:
    print("Directory {} Doesnt exist".format(paths['kv_path']))