from mlrun import get_or_create_ctx
from pyspark.sql import SparkSession
from os import path
import sys

def main(ctx):
    fuse_path = context.parameters['fuse_kv_path']
    kv_path = context.parameters['kv_path']
    parquet_path = context.parameters['parquet_path']
    #paths = generate_kv_parquet_path()
    ctx.logger.info("paths : {} {} {}".format(fuse_path, kv_path , parquet_path))
    if path.isdir(fuse_path):
        coalesce = 6
        # Initiate a Spark Session
        spark = SparkSession.builder.appName("Spark JDBC to Databases - ipynb").getOrCreate()

        df = spark.read.format('io.iguaz.v3io.spark.sql.kv').load(kv_path)
        df.show()
        df.repartition(coalesce).write.mode('overwrite').parquet(parquet_path)
    else:
        print("Directory {} Doesnt exist".format(fuse_path))
        sys.exit(1)


if __name__ == '__main__':
    context = get_or_create_ctx('run kv to parquet')
    main(context)
