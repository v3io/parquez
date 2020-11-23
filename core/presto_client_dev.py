from pyhive import presto  # or import hive
from core.params import Params


class PrestoClientDev(object):
    def __init__(self, logger, conf, params: Params):
        self.logger = logger
        self.partition_str = params.partition_by
        self.uri = conf.presto_uri
        self.user_name = params.user_name
        self.access_key = params.access_key
        self.hive_schema = conf.hive_schema
        self.cursor = None

    def connect(self):
        req_kw = {'auth': (self.user_name, self.access_key), 'verify': False}

        presto_cls = presto.connect(self.uri, port=443, username=self.user_name,
                                    protocol='https', requests_kwargs=req_kw)

        self.cursor = presto_cls.cursor()
        self.logger.info("connected to presto")

    def disconnect(self):
        self.cursor.close()

    def execute_command(self, command):
        self.logger.info("executing presto command : {}".format(command))
        self.cursor.execute(command)

    def fetch_results(self):
        return self.cursor.fetchall()

