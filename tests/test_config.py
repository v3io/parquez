from utils.logger import Logger
from config.app_conf import AppConf


# test_config.py


def test_app_conf():
    logger = Logger()
    cf = AppConf(logger,config_path='../config/parquez.ini')
    assert cf.v3io_container == "parquez"
    assert cf.v3io_connector == "v3io"
    assert cf.hive_connector == "hive"




