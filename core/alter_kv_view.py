import sys
from logger import Logger
from config.appconf import AppConf
from kv_table import KVTable
from kv_view import KVView

CONFIG_PATH = 'config/test_config.ini'


def main():
    logger = Logger()
    logger.info("altering view")

    args = sys.argv

    if len(args) > 3:
        config_path = args[3]
    else:
        config_path = CONFIG_PATH

    conf = AppConf(logger, config_path)
    conf.log_conf()

    logger.info("validating kv table")
    kvtable = KVTable(conf, args[1], logger)

    logger.info("generating view over kv")
    kv_view = KVView(logger, args[2], conf, kvtable)
    kv_view.generate_crete_view_script()


if __name__ == '__main__':
    main()
