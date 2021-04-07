import os
import configparser
import re


class AppConf(object):
    def __init__(self, logger, config_path='config/parquez.ini'):
        self.logger = logger
        files = os.listdir(os.curdir)
        self.logger.info(files)
        config = configparser.ConfigParser()
        config.read(config_path)
        self.v3io_container = config['v3io']['v3io_container']
        self.hive_schema = config['hive']['hive_schema']
        self.v3io_connector = config['presto']['v3io_connector']
        self.hive_connector = config['presto']['hive_connector']
        self.presto_uri = self.remove_https_prefix(uri=config['presto']['uri'])
        self.v3io_api_endpoint_host = self.remove_https_prefix(uri=config['nginx']['v3io_api_endpoint_host'])
        self.v3io_api_endpoint_port = config['nginx']['v3io_api_endpoint_port']
        self.compression = config['compression']['type']
        self.coalesce = config['compression']['coalesce']

    def presto_v3io_prefix(self):
        return self.v3io_connector + "." + self.v3io_container

    def presto_hive_prefix(self):
        return self.hive_connector + "." + self.hive_schema

    def remove_https_prefix(self, uri, prefix='https://'):
        if uri.startswith(prefix):
            ret_uri = uri[len(prefix):]
            self.logger.debug("removing https:// prefix of uri {}".format(uri))
            return ret_uri
        return uri
