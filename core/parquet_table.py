from core.table import Table
from core.params import Params
from config.app_conf import AppConf
from utils.logger import Logger
from utils.utils import Utils
from core.presto_client import PrestoClient
import requests
from os import path


# TODO: Add verification that hive table created (handle Trying to send on a closed client exception
def create_delete_external_table(logger: Logger, conf: AppConf, params: Params, request_type):
    url = get_external_location(conf, params)
    headers = get_request_headers(params.access_key)
    send_request(logger,
                 url,
                 headers, request_type)


def get_external_location(conf: AppConf, params: Params):
    return 'https://{}/{}/{}_Parquet/'.format(conf.v3io_api_endpoint_host,
                                              conf.v3io_container,
                                              params.real_time_table_name
                                              )


def get_request_headers(access_key):
    return {
        'Content-Type': 'application-octet-stream',
        'X-v3io-session-key': access_key
    }


def send_request(logger, url, headers, request_type):
    try:
        if request_type == 'PUT':
            response = requests.put(url, headers=headers, timeout=10, verify=False)
        if request_type == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=10, verify=False)
        logger.info(response.status_code)
        logger.debug(response.content)
        return response.content

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))


class ParquetTable(Table):
    def __init__(self, logger, conf: AppConf, params: Params, presto_client: PrestoClient):
        self.logger = logger
        self.conf = conf
        self.params = params
        self.presto_client = presto_client
        self.partition_str = params.partition_by
        self.partition_by_list = self.generate_partition_by_list()
        self.partition = []
        self.parquet_table_name = params.real_time_table_name + "_" + conf.compression
        self.compression = conf.compression
        self.table_name = "{}_{}".format(params.real_time_table_name, conf.compression)

    def split_parquet_path_to_values(self, path: str):
        split_values = path.split('/')
        ret_dict = {}
        for val in split_values:
            if val.startswith('year='):
                split_val = val.split('=')
                ret_dict['year'] = split_val[1]
            if val.startswith('month='):
                split_val = val.split('=')
                ret_dict['month'] = split_val[1]
            if val.startswith('day='):
                split_val = val.split('=')
                ret_dict['day'] = split_val[1]
            if val.startswith('hour='):
                split_val = val.split('=')
                ret_dict['hour'] = split_val[1]
        return ret_dict

    def create_with_clause_script(self):
        external_location = "'v3io://{}/{}/'".format(self.conf.v3io_container, self.table_name)
        suffix = "WITH (" \
                 " external_location = {}," \
                 " format = '{}', partitioned_by = ARRAY{}" \
                 ")" \
                 "".format(external_location, self.conf.compression, self.partition_by_list)
        self.logger.info(suffix)
        return suffix

    def generate_create_table_script(self):
        columns = self.get_table_schema()
        with_clause = self.create_with_clause_script()
        create_script = "CREATE TABLE hive.{}.{} {} {}".format(self.conf.hive_schema,
                                                               self.table_name,
                                                               columns,
                                                               with_clause)
        return create_script

    def generate_partition_by_list(self):
        part = self.partition_str
        self.logger.debug("generate_partition_by {}".format(part))
        if part == 'y':
            return ['year']
        if part == 'm':
            return ['year', 'month']
        if part == 'd':
            return ['year', 'month', 'day']
        if part == 'h':
            return ['year', 'month', 'day', 'hour']
        return None

    def get_table_schema(self):
        table_path = "v3io.{}.{}".format(self.conf.v3io_container, self.params.real_time_table_name)
        self.presto_client.connect()
        command = "SHOW COLUMNS FROM {}".format(table_path)
        self.presto_client.execute_command(command)
        response = self.presto_client.fetch_results()
        columns = "("
        for column in response:
            if column[0] not in self.partition_by_list:
                column_str = "{} {} ,".format(column[0], column[1])
                columns += column_str
        part = self.partition_str
        self.logger.debug("generate_partition_by {}".format(part))
        if part == 'y':
            columns += "year bigint"
        if part == 'm':
            columns += "year bigint ,month bigint"
        if part == 'd':
            columns += "year bigint ,month bigint, day bigint"
        if part == 'h':
            columns += "year bigint ,month bigint, day bigint , hour bigint"
        columns += ")"
        self.logger.info(columns)
        return columns

    def create(self):
        try:
            self.logger.debug("generating script")
            script = self.generate_create_table_script()
            create_delete_external_table(self.logger, self.conf, self.params, 'PUT')
            self.presto_client.connect()
            self.presto_client.execute_command(script)
            self.presto_client.fetch_results()
            self.presto_client.disconnect()
        except Exception as e:
            self.logger.error(e)
            raise

    def add_partition(self, year=None, month=None, day=None, hour=None):
        vals = ""
        if self.partition_str == 'y':
            vals += "['{}']".format(str(year))
        if self.partition_str == 'm':
            vals += "['{}','{}']".format(str(year), str(month))
        if self.partition_str == 'd':
            vals += "['{}','{}','{}']".format(str(year), str(month), str(day))
        if self.partition_str == 'h':
            vals += "['{}','{}','{}','{}']".format(str(year), str(month), str(day), str(hour))
        command_prefix = "CALL hive.system.create_empty_partition(schema_name =>'{}'" \
                         ",table_name =>'{}'" \
                         ",partition_columns=>ARRAY{}" \
                         ",partition_values =>ARRAY{})".format(self.conf.hive_schema,
                                                               self.table_name,
                                                               self.partition_by_list,
                                                               vals)
        self.presto_client.connect()
        self.presto_client.execute_command(command_prefix)
        self.presto_client.fetch_results()
        self.presto_client.disconnect()

    def drop_partition(self, year=None, month=None, day=None, hour=None):
        where_clause = ""
        if self.partition_str == 'y':
            where_clause += "year = {}]".format(year)
        if self.partition_str == 'm':
            where_clause += "year = {} AND month = {}".format(str(year), str(month))
        if self.partition_str == 'd':
            where_clause += "year = {} AND month = {} AND day={}".format(str(year), str(month), str(day))
        if self.partition_str == 'h':
            where_clause += "year = {} AND month = {} AND day={} AND hour ={}".format(year,
                                                                                      month,
                                                                                      day,
                                                                                      hour)
        command_prefix = "DELETE FROM hive.{}.{} " \
                         "WHERE {}".format(self.conf.hive_schema,
                                           self.table_name,
                                           where_clause)
        self.presto_client.connect()
        self.presto_client.execute_command(command_prefix)
        self.presto_client.fetch_results()
        self.presto_client.disconnect()

    def add_partition_from_path(self, partition_path):
        path_dict = self.split_parquet_path_to_values(partition_path)
        self.add_partition(year=path_dict['year'],
                           month=path_dict['month'],
                           day=path_dict['day'],
                           hour=path_dict['hour'])

    def drop_partition_from_path(self, partition_path):
        if path.isdir(partition_path):
            path_dict = self.split_parquet_path_to_values(partition_path)
            self.drop_partition(year=path_dict['year'],
                                month=path_dict['month'],
                                day=path_dict['day'],
                                hour=path_dict['hour'])
            utils = Utils(self.logger, self.conf)
            utils.delete_dir(partition_path)
        else:
            self.logger.info("path {} doesn't exist".format(partition_path))

    def drop(self):
        try:
            command = "DROP TABLE IF EXISTS hive.{}.{}".format(self.conf.hive_schema, self.parquet_table_name)
            self.presto_client.connect()
            self.presto_client.execute_command(command)
            self.presto_client.fetch_results()
            self.presto_client.disconnect()
        except Exception as e:
            self.logger.error(e)
            raise
