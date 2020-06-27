import requests
import json

SCHEMA = '.%23schema'
PARTITION_BY_FIELDS = ['year', 'month', 'day', 'hour']


def get_request_url(container_name, table_name, v3io_api_endpoint_host, v3io_api_endpoint_port):
    return 'https://{}:{}/{}/{}/{}'.format(v3io_api_endpoint_host, v3io_api_endpoint_port, container_name, table_name
                                          , SCHEMA)


def get_request_headers(access_key):
    return {
        'Content-Type': 'application/json',
        'X-v3io-session-key': access_key
    }


def send_request(logger, url, headers, username):
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        logger.debug(response.status_code)
        logger.debug(response.content)
        return response.content

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))


class KVTable(object):
    def __init__(self, logger, conf, name='table'):
        self.name = name
        self.logger = logger
        self.schema = "init_schema"
        self.conf = conf

    def import_table_schema(self):
        url = get_request_url(self.conf.v3io_container, self.name, self.conf.v3io_api_endpoint_host,
                              self.conf.v3io_api_endpoint_port)
        headers = get_request_headers(self.conf.v3io_access_key)
        schema = send_request(self.logger, url, headers, self.conf.username)
        self.logger.info('KV table schema {}'.format(schema))
        self.schema = schema
        return schema

    def get_schema_fields_and_types(self):
        js = json.loads(self.schema)
        fields = js['fields']
        parsed_schema = ""
        for ls in fields:
            field = ls['name']
            if field not in PARTITION_BY_FIELDS:
                field_type = ls['type']
                if field_type == 'long':
                    field_type = 'bigint'
                parsed_schema += field + ' ' + field_type + ',\n'
        parsed_schema = parsed_schema[:-2]
        self.logger.debug('schema_fields_and_types {}'.format(parsed_schema))
        return parsed_schema

    def get_schema_fields(self):
        js = json.loads(self.schema)
        fields = js['fields']
        parsed_schema = ""
        for ls in fields:
            field = ls['name']
            if field not in PARTITION_BY_FIELDS:
                parsed_schema += field + ' ,\n'
        parsed_schema = parsed_schema[:-2]
        self.logger.debug('schema_fields {}'.format(parsed_schema))
        return parsed_schema

    def get_parquet_table_name(self):
        parquet_name = self.name + '_'+self.conf.compression
        self.logger.debug('parquet table name {}'.format(parquet_name))
        return parquet_name



