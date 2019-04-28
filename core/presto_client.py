import re
#from core.logger import Logger
#from core.kv_view import KVView
#from core.parquet_table import ParquetTable
#from config.app_conf import AppConf
from pyhive import presto  # or import hive

STORED_AS_PARQUET_STR = " STORED AS PARQUET;"
PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


class PrestoClient(object):

    def __init__(self, logger, conf, parquet_table, kv_view, view_name="unified_view"):
        self.logger = logger
        self.uri = conf.presto_uri
        self.user_name = conf.username
        self.access_key = conf.v3io_access_key
        self.view_name = view_name
        self.kv_view_name = kv_view.name
        self.hive_schema = conf.hive_schema
        self.parquet_table_name = parquet_table.parquet_table_name
        self.cursor = None

    def connect(self):
        req_kw = {'auth': (self.user_name, self.access_key), 'verify': False}
        self.cursor = presto.connect(self.uri, port=443, username=self.user_name,
                                     protocol='https', requests_kwargs=req_kw).cursor()
        self.logger.info("connected to presto")

    def disconnect(self):
        self.cursor.close()

    def generate_unified_view(self):
        attributes = self.convert_schema()
        view = "CREATE OR REPLACE VIEW " + "hive." + self.conf.hive_schema + "." + self.view_name + " as ( SELECT " + attributes
        view += " FROM hive." + self.hive_schema + "." + self.kv_view_name
        view += " UNION ALL SELECT " + attributes
        view += " FROM hive." + self.hive_schema + "." + self.parquet_table_name
        view += ")"
        return view

    def generate_partition_by(self):
        part = re.match(PARTITION_INTERVAL_RE, self.partition_str).group(2)
        if part == 'y':
            partition_by = ",year"
        if part == 'm':
            partition_by = ",year,month"
        if part == 'd':
            partition_by = ",year,month,day"
        if part == 'h':
            partition_by = ",year,month,day,hour"
        return partition_by

    def convert_schema(self):
        schema_fields = self.kv_table.get_schema_fields()
        schema_fields += self.generate_partition_by()
        return schema_fields

    def execute_command(self, command):
        self.cursor.execute(command)

    def generate_unified_view(self):
        script = self.generate_unified_view()
        self.execute(script)
        self.logger.debug(script)