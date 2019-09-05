import os


class Utils(object):
    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf

    def copy_to_v3io(self, file):
        command = 'curl https://' + self.conf.v3io_api_endpoint_host + '/' + self.conf.v3io_container + '/ -H \'x-v3io-session-key: ' + self.conf.v3io_access_key + '\' --insecure --upload-file ' + file
        self.logger.info('copy command %s ' % command)
        os.system(command)
