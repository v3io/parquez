#!/usr/local/bin/python

# Locate v3iod:
from subprocess import run

run(["/bin/bash", "/etc/config/v3io/v3io-spark-operator.sh"])


def generate_kv_parquet_path(container='parquez', table='faker', compress_type='parquet'):
    container = 'parquez'
    table = 'faker'
    compress_type = 'parquet'

    from datetime import datetime, timezone
    curent_date_path = datetime.now(timezone.utc).strftime("year=%Y/month=%m/day=%d/hour=%H")
    kv_path = "v3io://{}/{}/{}".format(container, table, curent_date_path)
    print(kv_path)
    parquet_path = "v3io://{}/{}_{}/{}".format(container, table, compress_type, curent_date_path)
    print(parquet_path)
    return {'kv_path': kv_path, 'parquet_path': parquet_path}


coalesce = 6

# The pyspark code:
import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages mysql:mysql-connector-java:5.1.39 pyspark-shell"

spark = SparkSession.builder.appName("Spark JDBC to Databases - ipynb").config("spark.driver.extraClassPath",
                                                                               "/v3io/users/admin/mysql-connector-java-5.1.45.jar").config(
    "spark.executor.extraClassPath", "/v3io/users/admin/mysql-connector-java-5.1.45.jar").getOrCreate()
paths = generate_kv_parquet_path()
df = spark.read.format('io.iguaz.v3io.spark.sql.kv').load(paths['kv_path'])
df.show()
df.repartition(coalesce).write.mode('overwrite').parquet(paths['parquet_path'])