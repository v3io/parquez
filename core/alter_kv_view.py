import sys
from utils.logger import Logger
from kv_table import KVTable
from kv_view import KVView

CONFIG_PATH = 'config/parquez.ini'


def parse_kv_window_arg(val, logger):
    val = val.replace('hours', 'h')
    val = val.replace('days', 'd')
    val = val.replace(" ", "")
    logger.debug("parsed kv window val " + val)
    return val


def main():
    logger = Logger()
    logger.info("altering view")

    args = sys.argv

    conf = AppConf(logger, CONFIG_PATH)
    conf.log_conf()

    logger.info("validating kv table " + args[1])
    kv_table = KVTable(conf, args[1], logger)

    logger.info("generating view over kv" + args[2])
    parsed_window = parse_kv_window_arg(args[2], logger)
    kv_view = KVView(logger, parsed_window, conf, kv_table)
    kv_view.generate_crete_view_script()


if __name__ == '__main__' and __package__ is None:
    if __package__ is None:
        from os import path

        sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
        from config.app_conf import AppConf
    else:
        from ..config.app_conf import AppConf
    main()
