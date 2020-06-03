
HIVE_PREFIX = "/hive/bin/hive -hiveconf hive.metastore.uris=thrift://hive:9083 -e "
STORED_AS_PARQUET_STR = " STORED AS PARQUET;"
PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


# TODO: Add verification that hive table created (handle Trying to send on a closed client exception


class ParquetTable(object):
    def __init__(self, logger, conf, utils, partition_by, kv_table, k8s_client):
        self.logger = logger
        self.kv_table = kv_table
        self.partition_str = partition_by
        self.partition_by = " PARTITIONED BY ("
        self.partition = []
        self.parquet_table_name = kv_table.name + "_" + conf.compression
        self.conf = conf
        self.utils = utils
        self.compression = conf.compression
        self.k8s_client = k8s_client

    def generate_create_table_script(self):
        self.logger.debug("generate_create_table_script")
        parquet_script = "CREATE EXTERNAL TABLE " + self.conf.hive_schema + '.' + self.parquet_table_name + " ("
        return parquet_script

    def generate_partition_by(self):
        part = self.partition_str
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
        schema_str = self.kv_table.get_schema_fields_and_types()
        schema_str += ")"
        self.logger.debug("read schema {0}".format(schema_str))
        return schema_str

    def generate_script(self):
        try:
            self.logger.debug("generating script")
            parquet_command = HIVE_PREFIX
            parquet_command += '"'+self.generate_create_table_script()
            parquet_command += self.read_schema()
            parquet_command += self.generate_partition_by()
            parquet_command += " STORED AS "+self.compression+';"'
            self.logger.info(parquet_command)
            self.k8s_client.exec_shell_cmd(parquet_command)
        except Exception as e:
            self.logger.error(e)
            raise
