# Copyright 2017 The Nuclio Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import nuclio_sdk
import requests
import json


WEB_API = 'webapi.default-tenant.app.dev39.lab.iguazeng.com'
ACCESS = '9cb6a81c-4fab-482a-bfc0-acae9bcb1e53'
CONTAINER = 'parquez'
KV_TABLE_NAME = 'faker'
SCHEMA = '.%23schema'
PARTITION_BY_FIELDS = ['year', 'month', 'day', 'hour']


def get_request_url(container_name, table_name, v3io_api_endpoint_host):
    return 'https://{}/{}/{}/{}'.format(v3io_api_endpoint_host, container_name, table_name
                                           , SCHEMA)


def get_request_headers():
    return {
        'Content-Type': 'application/json',
        "X-v3io-session-key": ACCESS
    }


def send_request(logger, url, headers):
    try:
        response = requests.get(url, headers=headers, verify=False)
        logger.debug(response.status_code)
        logger.debug(response.content)
        return response.content

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))


def get_table_schema(context,
                     container: str = 'parquez',
                     kv_table_name: str = 'faker',
                     target_path :str = './'):
    """Open a file/object archive into a target directory

    :param target_dir:   target directory
    :param archive_url:  source archive path/url

    :returns: content dir
    """

    kv_table = KVTable(context.logger, container,  kv_table_name)
    content = kv_table.import_table_schema()
    context.logger.info(str(content))
    schema_path = target_path+"schema.txt"
    with open(schema_path, "w") as text_file:
        text_file.write(str(content))
    #context.logger.info(f'extracted archive to {target_dir}')
    context.log_artifact('content', local_path=target_path)


class KVTable(object):
    def __init__(self, logger, container, name='table'):
        self.name = name
        self.container = container
        self.logger = logger

    def import_table_schema(self):
        url = get_request_url(self.container, self.name, WEB_API)
        headers = get_request_headers()
        schema = send_request(self.logger, url, headers)
        self.logger.info('KV table schema {}'.format(schema))
        return str(schema)

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
        parquet_name = self.name + '_' + self.conf.compression
        self.logger.debug('parquet table name {}'.format(parquet_name))
        return parquet_name

from mlrun.runtimes.function import fake_nuclio_context
log = nuclio_sdk.logger.Logger(level=1)
ctx, event = fake_nuclio_context(
    "", headers="")
# init_context(ctx)
get_table_schema(ctx)


