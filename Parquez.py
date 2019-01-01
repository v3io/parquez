from InputParser import InputParser
from logger import Logger
from ParquetTableGenerator import ParquetTableGenerator
from Presto import Presto
from CronGenerator import CronGenerator
from config import Config


def main():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    conf = Config(logger)
    conf.log_conf()

    logger.info("Parsing data")
    parser = InputParser(logger)

    logger.info("generating parquet table")
    parquet = ParquetTableGenerator(logger, parser.kv_table_name, parser.schema_path, parser.partition_by, conf)
    parquet.generate_script()

    logger.info("generating presto view")
    prest = Presto(logger, parser.view_name, parser.partition_by, parser.schema_path, conf.presto_v3io_prefix(), parser.kv_table_name,
                   conf.presto_hive_prefix(), parquet.parquet_table_name, conf)
    prest.execute_command()

    logger.info("generating cronJob")
    cr = CronGenerator(logger, conf, parser.kv_table_name, parser.partition_interval, parser.key_value_window,
                       parser.historical_retention, parser.partition_by)
    cr.create_cron_job()


if __name__ == '__main__':
    main()










