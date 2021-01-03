from mlrun import get_or_create_ctx, new_function

def main(context):
    partition = context.parameters['kv_path']
    context.logger.info("deleting partition {}".format(partition))
    sj = new_function(kind='spark',
                      command='/igz/java/libs/v3io-spark2-tools_2.11-546107605231899641401.jar',
                      name='delete_kv_partition')
    sj.with_driver_limits(cpu="1300m")
    sj.with_driver_requests(cpu=1, mem="512m")
    sj.with_executor_limits(cpu="1400m")
    sj.with_executor_requests(cpu=1, mem="512m")
    # Not really needed in this case
    sj.with_igz_spark()
    sj.spec.replicas = 2
    sj.spec.job_type = "Scala"
    sj.spec.main_class = "io.iguaz.v3io.spark2.tools.DeleteTable"
    sj.spec.args = [partition]
    sr = sj.run(artifact_path="/User/artifacts")


if __name__ == '__main__':
    # kv_partition = generate_kv_parquet_path()
    ctx = get_or_create_ctx('delete_partition')    
    main(ctx)
