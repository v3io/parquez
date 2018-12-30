import ConfigParser


class Config:

    def __init__(self, logger, config_path='parquez.cfg'):
        self.logger = logger
        self.configPath = config_path
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configPath)
        self.v3io_container = self.config.get('v3io', 'v3io_container')
        self.v3io_path = self.config.get('v3io', 'v3io_path')

        self.hive_home = self.config.get('hive', 'hive_home')
        self.hive_schema = self.config.get('hive', 'hive_schema')

        self.v3io_connector = self.config.get('presto', 'v3io_connector')
        self.hive_connector = self.config.get('presto', 'hive_connector')

    def log_conf(self):
        self.logger.info(self.hive_home)

    def presto_v3io_prefix(self):
        return self.v3io_connector+"."+self.v3io_container

    def presto_hive_prefix(self):
        return self.hive_connector+"."+self.hive_schema



