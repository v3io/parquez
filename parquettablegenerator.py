import re
from logger import Logger

STORED_AS_PARQUET_STR = " STORED AS PARQUET;"
PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"

# TODO: Add verification that hive table created (handle Trying to send on a closed client exception


class ParquetTableGenerator(object):
    def __init__(self, logger, kv_table_name, schema_path, partition_by, conf):
        self.logger = logger
        self.kv_table_name = kv_table_name
        self.schema = schema_path
        self.partition_str = partition_by
        self.partition_by = " PARTITIONED BY ("
        self.partition = []
        self.parquet_table_name = kv_table_name+"_parquet"
        self.conf = conf

    def generate_create_table_script(self):
        self.logger.debug("generate_create_table_script")
        parquet_script = "CREATE EXTERNAL TABLE "+self.conf.hive_schema+'.'+self.parquet_table_name+" ("
        return parquet_script

    def generate_partition_by(self):
        part = re.match(PARTITION_INTERVAL_RE, self.partition_str).group(2)
        self.logger.debug("generate_partition_by {0}".format(part))
        if part == 'y':
            self.partition_by += "year int"
        if part == 'm':
            self.partition_by += "year int,month int"
        if part == 'd':
            self.partition_by += "year int,month int,day int"
        if part == 'h':
            self.partition_by += "year int,month int,day int ,hour int"
        self.partition_by += ")"
        return self.partition_by

    def read_schema(self):
        self.logger.debug("read schema {0}".format(self.schema))
        file = open(self.schema, "r")
        schema_str = file.read()
        schema_str += ")"
        return schema_str

    def create_table(self):
        import os
        hive_path = self.conf.hive_home
        command = hive_path + " -f create_table.txt"
        self.logger.info("Create Hive table command : " + command)
        os.system(command)

    def generate_script(self):
        try:
            self.logger.debug("generating script")
            parquet_command = self.generate_create_table_script()
            parquet_command += self.read_schema()
            parquet_command += self.generate_partition_by()
            parquet_command += " STORED AS PARQUET;"
            self.logger.debug("create table script {}".format(parquet_command))
            f = open("create_table.txt", "w")
            f.write(parquet_command)
            f.close()
            self.create_table()
        except Exception as e:
            self.logger.error(e)
            raise






