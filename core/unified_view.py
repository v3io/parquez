from datetime import datetime, timedelta
import re
from config.app_conf import AppConf
from core.presto_client import PrestoClient
from core.params import Params

PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


class UnifiedView(object):
    def __init__(self, logger, params: Params, conf:AppConf, schema, presto_client: PrestoClient):
        self.logger = logger
        self.params = params
        self.conf = conf
        self.schema = schema
        self.presto_client = presto_client

    def generate_unified_view_script(self):
        attributes = self.convert_schema()
        view = "CREATE OR REPLACE VIEW " + "hive." + self.conf.hive_schema + "." + self.params.view_name + " as ( SELECT " + attributes
        view += " FROM hive." + self.conf.hive_schema + "." + self.params.real_time_table_name+"_view"
        view += " UNION ALL SELECT " + attributes
        view += " FROM hive." + self.conf.hive_schema + "." + self.params.real_time_table_name+"_"+self.conf.compression
        view += ")"
        self.logger.info("unified view script : {}".format(view))
        return view

    def convert_schema(self):
        schema_fields = self.schema
        schema_fields += self.generate_partition_by()
        return schema_fields

    def generate_partition_by(self):
        part = self.params.partition_by
        if part == 'y':
            partition_by = ",year"
        if part == 'm':
            partition_by = ",year,month"
        if part == 'd':
            partition_by = ",year,month,day"
        if part == 'h':
            partition_by = ",year,month,day,hour"
        return partition_by

    def execute_script_in_presto(self):
        script = self.generate_unified_view_script()
        self.presto_client.connect()
        self.presto_client.execute_command(script)
        self.logger.info(self.presto_client.fetch_results())
        self.presto_client.disconnect()

    def drop_view(self):
        try:
            self.logger.info("dropping unified view ")
            hive_prefix = "hive." + self.conf.hive_schema + "."
            command = "DROP VIEW IF EXISTS " + hive_prefix + self.params.view_name
            self.presto_client.connect()
            self.presto_client.execute_command(command)
            self.logger.info(self.presto_client.fetch_results())
            self.presto_client.disconnect()
        except Exception as e:
            self.logger.error(e)
            raise

