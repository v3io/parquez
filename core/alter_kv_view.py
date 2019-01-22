import sys
from logger import Logger
from kv_table import KVTable
from kv_view import KVView

CONFIG_PATH = 'config/test_config.ini'


def main():
    logger = Logger()
    logger.info("altering view")

    args = sys.argv

    conf = AppConf(logger, CONFIG_PATH)
    conf.log_conf()

    logger.info("validating kv table "+args[1])
    kv_table = KVTable(conf, args[1], logger)
    #kv_table.import_table_schema()

    logger.info("generating view over kv"+args[2])
    kv_view = KVView(logger, args[2], conf, kv_table)
    kv_view.generate_crete_view_script()


if __name__ == '__main__' and __package__ is None:
    if __package__ is None:
        from os import path
        sys.path.append( path.dirname(path.dirname( path.abspath(__file__) ) ) )
        from config.app_conf import AppConf
    else:
        from ..config.app_conf import AppConf
    main()
