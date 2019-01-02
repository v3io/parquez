from inputparser import InputParser
from logger import Logger
from parquettablegenerator import ParquetTableGenerator
from presto import Presto
from crontab import Crontab
from config.appconf import AppConf


def main():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    conf = AppConf(logger)
    conf.log_conf()

    logger.info("Parsing data")
    parser = InputParser(logger)
    args = parser.parse_args()

    logger.info("generating parquet table")
    parquet = ParquetTableGenerator(logger, args.real_time_table_name, args.schema_path, args.partition_by, conf)
    parquet.generate_script()

    logger.info("generating presto view")
    prest = Presto(logger, args.view_name, args.partition_by, args.schema_path, conf.presto_v3io_prefix(), args.real_time_table_name,
                   conf.presto_hive_prefix(), parquet.parquet_table_name, conf)
    prest.execute_command()

    logger.info("generating cronJob")
    cr = Crontab(logger, conf, args.real_time_table_name, args.partition_interval, args.key_value_window,
                 args.historical_retention, args.partition_by)
    cr.create_cron_job()


if __name__ == '__main__':
    main()










