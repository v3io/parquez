from core.logger import Logger
from config.app_conf import AppConf
from config.utils import Utils


# test_config.py


def test_generate():
    logger = Logger()
    cf = AppConf(logger, config_path='test.ini')
    setup = Utils(logger, cf)
    setup.copy_to_v3io("../v3io-spark2-tools_2.11.jar")





