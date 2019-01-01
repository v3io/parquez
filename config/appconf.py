import os
import configparser
from logger import Logger


class AppConf:

    def __init__(self, logger: Logger, config_path='config/parquez.ini'):
        self.logger = logger
        config = configparser.ConfigParser()
        config.read(config_path)
        self.logger.debug(os.getcwd())
        self.v3io_container = config['v3io']['v3io_container']
        self.v3io_path = config['v3io']['v3io_path']
        self.hive_home = config['hive']['hive_home']
        self.hive_schema = config['hive']['hive_schema']
        self.v3io_connector = config['presto']['v3io_connector']
        self.hive_connector = config['presto']['hive_connector']

    def log_conf(self):
        self.logger.info(self.hive_home)

    def presto_v3io_prefix(self):
        return self.v3io_connector+"."+self.v3io_container

    def presto_hive_prefix(self):
        return self.hive_connector+"."+self.hive_schema







