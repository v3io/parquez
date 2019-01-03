from logger import Logger
import requests
import json

V3IO_API_ENDPOINT_HOST = '192.168.206.1'
V3IO_API_ENDPOINT_PORT = '8081'
USERNAME = 'iguazio'
PASSWORD = 'datal@ke!'
SCHEMA = '.%23schema'
PARTITION_BY_FIELDS = ['year', 'month', 'day', 'hour']


def get_request_url(container_name, table_name):
    return 'http://{}:{}/{}/{}/{}'.format(V3IO_API_ENDPOINT_HOST,V3IO_API_ENDPOINT_PORT,container_name,table_name,SCHEMA)


def get_request_headers():
    return {
        'Content-Type': 'application/json',
    }


def send_request(logger, url, headers):
    try:
        auth = requests.auth.HTTPBasicAuth(USERNAME, PASSWORD)
        response = requests.get(url, headers=headers, auth=auth, timeout=1)

        logger.debug(response.status_code)
        logger.debug(response.content)
        return response.content

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))


class KVTable:

    def __init__(self, container_name= 'default', name='table', logger=Logger()):
        self.container_name = container_name
        self.name = name
        self.logger = logger
        self.schema = self.import_table_schema()

    def import_table_schema(self):
        url = get_request_url(self.container_name, self.name)
        headers = get_request_headers()
        schema = send_request(self.logger, url, headers)
        self.logger.info('KV table schema {}'.format(schema))
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
            parsed_schema += field + ',\n'
        parsed_schema = parsed_schema[:-2]
        self.logger.debug('schema_fields {}'.format(parsed_schema))
        return parsed_schema

    def get_parquet_table_name(self):
        parquet_name = self.name + '_parquet'
        self.logger.debug('parquet table name {}'.format(parquet_name))
        return parquet_name



