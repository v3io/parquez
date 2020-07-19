from utils.logger import Logger
from config.app_conf import AppConf
from core.cron_tab import CronTab
from core.params import Params

# test_config.py


def test_crontab():
    logger = Logger()
    conf = AppConf(logger, config_path='test.ini')
    params = Params()
    params.view_name = "view_name"
    params.partition_by = 'h'
    params.partition_interval = '1h'
    params.real_time_window = '1d'
    params.historical_retention = '7d'
    params.real_time_table_name = "real_time_table_name"
    cr = CronTab(logger, conf, params)
    cron_str = cr.create_cron_string()
    logger.info(cron_str)
    cron_command = cr.run_command()
    logger.info(cron_command)


