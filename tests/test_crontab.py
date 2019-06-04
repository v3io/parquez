from utils.logger import Logger
from core.parquet_table import ParquetTable
from config.app_conf import AppConf
from core.cron_tab import Crontab
from core.presto_client import PrestoClient

# test_config.py


def test_crontab():
    logger = Logger()
    conf = AppConf(logger, config_path='../config/parquez.ini')
    parquet = ParquetTable(logger, 'kv_table_name', 'schema.txt', '1h', conf)

    parquet.generate_script()
    prest = PrestoClient(logger, 'view_name', '1h', 'schema.txt', conf.presto_v3io_prefix(),'kv_table_name',
                   conf.presto_hive_prefix(), parquet.parquet_table_name, conf)
    prest.execute_command()
    cr = Crontab(logger, conf, 'kv_table_name','1h', '3h',
                 '7h', '1h')
    cr.create_cron_job()
