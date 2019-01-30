import os
import re

PRESTO_COMMAND_PREFIX = "/opt/presto/bin/presto-cli --server=https://localhost:8889 --catalog v3io " \
                 "--password --truststore-path /opt/presto/ssl/presto.jks " \
                 "--truststore-password sslpassphrase " \
                 "--user iguazio " \

STORED_AS_PARQUET_STR = " STORED AS PARQUET;"
PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


class Presto(object):
    def __init__(self, logger, view_name, partition_str, conf, kv_table, kv_view):
        self.logger = logger
        self.view_name = view_name
        self.partition_str = partition_str
        self.second_table_schema = conf.hive_schema
        self.second_table = kv_table.get_parquet_table_name()
        self.conf = conf
        self.kv_table = kv_table
        self.kv_view = kv_view

    @property
    def generate_unified_view(self):
        attributes = self.convert_schema()
        view = "CREATE OR REPLACE VIEW " + "hive." + self.conf.hive_schema + "." + self.view_name + " as ( SELECT " + attributes
        view += " FROM hive." + self.conf.hive_schema + "." + self.kv_view.name
        view += " UNION ALL SELECT " + attributes
        view += " FROM hive." + self.second_table_schema + "." + self.second_table
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

    def execute_command(self):
        script = self.generate_unified_view()
        self.logger.debug(script)
        prefix = self.get_presto_password()
        full_command = prefix + self.generate_presto_command_with_user() + script + "\""
        self.logger.debug("Presto full command {}".format(full_command))
        os.system(full_command)

    def get_presto_password(self):
        presto_command_prefix = ''
        if self.conf.v3io_access_key != '<access_key>' or self.conf.v3io_access_key is not None:
            presto_command_prefix = 'PRESTO_PASSWORD=' + self.conf.v3io_access_key + ' '
            self.logger.debug("Presto command prefix {}".format(presto_command_prefix))
        return presto_command_prefix

    def generate_presto_command_with_user(self):
        command = PRESTO_COMMAND_PREFIX + "--user " + self.conf.username + "--execute \" "
        self.logger.debug("Presto command prefix with user {}".format(command))
        return command
