import os
import re

STORED_AS_PARQUET_STR = " STORED AS PARQUET;"
PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


class Presto:

    def __init__(self, logger, view_name, partition_str, schema_path, first_table_schema, first_table, second_table_schema, second_table, conf):
        self.logger = logger
        self.view_name = view_name
        self.partition_str = partition_str
        self.first_table_schema = first_table_schema
        self.first_table = first_table
        self.second_table_schema = second_table_schema
        self.second_table = second_table
        self.schema = schema_path
        self.converted_schema = "converted_"+schema_path
        self.conf = conf
        self.convert_schema()

    def generate_unified_view(self):
        attributes = self.read_schema(self.converted_schema)
        str = "CREATE VIEW "+self.view_name+" as ( SELECT "+attributes
        str += " FROM "+self.first_table_schema+"."+self.first_table
        str += " UNION ALL SELECT "+attributes
        str += " FROM "+self.second_table_schema+"."+self.second_table
        str += ")"
        return str

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

    def read_schema(self,path):
        file = open(path, "r")
        schema_str = file.read()
        return schema_str

    def convert_schema(self):
        with open(self.schema) as fp:
            with open("converted_schema.txt","w") as wp:
                line = fp.readline()
                cnt = 1
                while line:
                    index = line.strip().find(' ')
                    l2 = line[0:index+2]
                    line = fp.readline()
                    if line:
                        l2 += ",\n"
                    wp.write(l2)

                    cnt += 1
                str = self.generate_partition_by()
                wp.write(str)

    def stored_by_parquet(self):
        str = " STORED AS PARQUET;"
        return str

    def execute_command(self):
        script = self.generate_unified_view()
        self.logger.debug(script)
        os.system("/opt/presto/bin/presto-cli.sh --server http://localhost:8889 --catalog hive --schema default --execute \"" + script + "\"")


