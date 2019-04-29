import os


class Setup(object):
    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf

    def copy_to_v3io(self):
        command = 'curl https: //' + self.conf.v3io_api_endpoint_host + '/parquez/ -H \'x-v3io-session-key: ' + self.conf.v3io_access_key + '\' --insecure --upload-file v3io-spark2-tools_2.11.jar"'
        os.system(command)
