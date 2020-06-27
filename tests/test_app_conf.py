from utils.logger import Logger
from config.app_conf import AppConf


# test_app_conf.py
def test_app_conf():
    logger = Logger()
    cf = AppConf(logger, config_path='../config/parquez.ini')
    assert cf.v3io_container == "bigdata"
    assert cf.v3io_access_key== "<access_key>"
    assert cf.hive_schema == "default"
    assert cf.presto_uri == "<localhost>"
    assert cf.v3io_connector == "v3io"
    assert cf.hive_connector == "hive"
    assert cf.v3io_api_endpoint_host == "<localhost>"
    assert cf.v3io_api_endpoint_port == "8081"
    assert cf.username == "<user_name>"

