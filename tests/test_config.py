from logger import Logger
from config.appconf import AppConf


# test_config.py


def test_generate():
    logger = Logger()
    cf = AppConf(logger,config_path='../config/parquez.ini')
    assert cf.v3io_container == "parquez"
    assert cf.v3io_path == "v3io://parquez"
    assert cf.hive_home == "/opt/hive/bin/hive"
    assert cf.v3io_connector == "v3io"
    assert cf.hive_connector == "hive"




