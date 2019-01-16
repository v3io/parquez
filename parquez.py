from core.input_parser import InputParser
from core.logger import Logger
from core.parquet_table import ParquetTable
from core.presto import Presto
from core.cron_tab import Crontab
from config.appconf import AppConf
from core.kv_table import KVTable


CONFIG_PATH = 'config/parquez.ini'


def main():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    logger.info("Parsing data")
    parser = InputParser(logger)
    args = parser.parse_args()

    if args.config is not None:
        config_path = args.config
    else:
        config_path = CONFIG_PATH

    conf = AppConf(logger, config_path)
    conf.log_conf()

    logger.info("validating kv table")
    kvtable = KVTable(conf.v3io_container, args.real_time_table_name, logger)

    logger.info("generating parquet table")
    parquet = ParquetTable(logger, args.partition_by, conf, kvtable)
    parquet.generate_script()

    logger.info("generating presto view")
    prest = Presto(logger, args.view_name, args.partition_by, conf,kvtable)
    prest.execute_command()

    logger.info("generating cronJob")
    cr = Crontab(logger, conf, args.real_time_table_name, args.partition_interval, args.real_time_window,
                 args.historical_retention, args.partition_by)
    cr.create_cron_job()


if __name__ == '__main__':
    main()










