from logger import Logger
import requests
import json

V3IO_API_ENDPOINT_HOST = '192.168.206.1'
V3IO_API_ENDPOINT_PORT = '8081'
USERNAME = 'iguazio'
PASSWORD = 'datal@ke!'
SCHEMA = '.%23schema'


def _get_request_url(container_name, table_name):
    return 'http://{}:{}/{}/{}/{}'.format(V3IO_API_ENDPOINT_HOST,V3IO_API_ENDPOINT_PORT,container_name,table_name,SCHEMA)


def _get_request_headers():
    return {
        'Content-Type': 'application/json',
    }


def _send_request(logger, url, headers):
    try:
        auth = requests.auth.HTTPBasicAuth(USERNAME, PASSWORD)
        response = requests.get(url, headers=headers, auth=auth, timeout=1)

        logger.debug(response.status_code)
        logger.debug(response.content)
        return response.content

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))


class KVTable():

    def __init__(self, container_name, name, logger=Logger()):
        self.container_name = container_name
        self.name = name
        self.logger = logger

    def get_table_schema(self):
        url = _get_request_url(self.container_name, self.name)
        headers = _get_request_headers()
        schema = _send_request(self.logger, url, headers)
        parsed_schema = self.parse_schema(schema)
        self.logger.debug(parsed_schema)
        return parsed_schema

    def parse_schema(self, schema):
        js = json.loads(schema)
        fields = js['fields']
        parsed_schema = ""
        for ls in fields:
            name = ls['name']
            parsed_schema += name + ',\n'
            self.logger.debug(name)
        parsed_schema = parsed_schema[:-2]
        return parsed_schema



