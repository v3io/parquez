from core.input_parser import InputParser
from utils.logger import Logger
from core.parquet_table import ParquetTable
from core.cron_tab import Crontab
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.presto_client import PrestoClient
from utils.utils import Utils

CONFIG_PATH = 'config/parquez.ini'


def main():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    parser = InputParser(logger)
    args = parser.parse_args()
    logger.info("input parsed")

    if args.config is not None:
        config_path = args.config
    else:
        config_path = CONFIG_PATH

    conf = AppConf(logger, config_path)
    logger.info("loading configuration")

    # should be deleted from 2.3 versions
    logger.info("initializing setup")
    setup = Utils(logger, conf)
    setup.copy_to_v3io("v3io-spark2-tools_2.11.jar")

    logger.info("validating kv table")
    kv_table = KVTable(logger, conf, args.real_time_table_name)
    kv_table.import_table_schema()

    logger.info("generating parquet table")
    parquet = ParquetTable(logger,setup, args.partition_by, conf, kv_table)
    parquet.generate_script()

    logger.info("generating view over kv")
    kv_view = KVView(logger, args.partition_by, conf, kv_table)
    kv_view.generate_crete_view_script()

    #logger.info("generating presto view")
    #prest = Presto(logger, args.view_name, args.partition_by, conf, kv_table, kv_view)
    #prest.execute_command()

    logger.info("generating presto view")
    prest = PrestoClient(logger, conf, args.partition_by, parquet, kv_view, kv_table)
    prest.generate_unified_view()


    logger.info("generating cronJob")
    cr = Crontab(logger, conf, args.real_time_table_name, args.partition_interval, args.real_time_window,
                 args.historical_retention, args.partition_by)
    cr.create_cron_job()


if __name__ == '__main__':
    main()










