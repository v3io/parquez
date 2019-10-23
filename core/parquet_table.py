K8S = 'kubectl -n default-tenant exec $(kubectl -n default-tenant get pods --no-headers -o ' \
      'custom-columns=":metadata.name" | grep shell)  -- /bin/bash -c "/hive/bin/hive -hiveconf ' \
      'hive.metastore.uris=thrift://hive:9083 '
VANILLA = '/opt/hive/bin/hive '
STORED_AS_PARQUET_STR = " STORED AS PARQUET;"
PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


# TODO: Add verification that hive table created (handle Trying to send on a closed client exception


class ParquetTable(object):
    def __init__(self, logger, conf, utils, partition_by, kv_table):
        self.logger = logger
        self.kv_table = kv_table
        self.partition_str = partition_by
        self.partition_by = " PARTITIONED BY ("
        self.partition = []
        self.parquet_table_name = kv_table.name + "_" + conf.compression
        self.conf = conf
        self.utils = utils
        self.compression = conf.compression
        self.hive_prefix = self.set_hive_prefix()

    def generate_create_table_script(self):
        self.logger.debug("generate_create_table_script")
        parquet_script = "CREATE EXTERNAL TABLE " + self.conf.hive_schema + '.' + self.parquet_table_name + " ("
        return parquet_script

    def generate_partition_by(self):
        part = self.partition_str
        self.logger.debug("generate_partition_by {0}".format(part))
        if part == 'y':
            self.partition_by += "year bigint"
        if part == 'M':
            self.partition_by += "year bigint,month bigint"
        if part == 'd':
            self.partition_by += "year bigint,month bigint,day bigint"
        if part == 'h':
            self.partition_by += "year bigint,month bigint,day bigint ,hour bigint"
        self.partition_by += ")"
        return self.partition_by

    def read_schema(self):
        schema_str = self.kv_table.get_schema_fields_and_types()
        schema_str += ")"
        self.logger.debug("read schema {0}".format(schema_str))
        return schema_str

    def create_table(self):
        import os
        command = self.hive_prefix + " -f 'v3io://" + self.conf.v3io_container + "/create_table.txt' \""
        self.logger.info("Create Hive table command : " + command)
        os.system(command)

    def copy_to_v3io(self):
        self.utils.copy_to_v3io("create_table.txt")

    def generate_script(self):
        try:
            self.logger.debug("generating script")
            parquet_command = self.generate_create_table_script()
            parquet_command += self.read_schema()
            parquet_command += self.generate_partition_by()
            parquet_command += " STORED AS " + self.compression + ";"
            self.logger.debug("create table script {}".format(parquet_command))
            f = open("create_table.txt", "w")
            f.write(parquet_command)
            f.close()
            self.copy_to_v3io()
            self.create_table()
        except Exception as e:
            self.logger.error(e)
            raise

    def set_hive_prefix(self):
        if self.conf.environment == 'k8s':
            return K8S
        if self.conf.environment == 'vanilla':
            return VANILLA
