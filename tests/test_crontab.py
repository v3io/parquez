from utils.logger import Logger
from config.app_conf import AppConf
from core.cron_tab import CronTab

# test_config.py


def test_crontab():
    logger = Logger()
    conf = AppConf(logger, config_path='test.ini')
    cr = CronTab(logger, conf, 'kv_table_name', '1h', '3h',
                 '7h', '1h')
    cr.create_cron_job()
