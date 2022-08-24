# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# coding: utf-8

import argparse

# TODO: 2. remove self values

PARTITION_BY_CHOICES = ['M', 'd', 'h']

TIME_CHOICES = ['1h', '2h', '3h', '4h', '5h', '6h', '7h', '8h', '9h', '10h',
                '11h', '12h', '13h', '14h', '15h', '16h', '17h', '18h', '19h',
                '20h', '21h', '22h', '23h', '24h',
                '1M', '2M', '3M', '4M', '5M', '6M', '7M', '8M', '9M', '10M', '11M', '12M',
                '1d', '2d', '3d', '4d', '5d', '6d', '7d', '8d', '9d', '10d', '11d', '12d',
                '13d', '14d', '15d', '16d', '17d', '18d', '19d', '20d', '21d', '22d', '23d', '24d',
                '25d', '26d', '27d', '28d', '29d', '30d'
                ]


class InputParser(object):

    def __init__(self, logger):
        self.logger = logger
        self.parser = self.create_parser()

    def create_parser(self):
        try:
            parser = argparse.ArgumentParser("Parquez"
                                              , description='Parquez a mechanism for storing fresh/hot data in the '
                                                            'NoSQL database and historical data on Parquet '
                                              , usage='use runParquez --help for more information'
                                              , formatter_class=argparse.RawTextHelpFormatter)

            parser.add_argument('--view-name', help='Parquez unified view '
                                     , metavar='view_name'
                                     , required=True)
            parser.add_argument('--partition-by'
                                     , help='Partition by [y / M / d / h] '
                                     , metavar='partition_by'
                                     , choices=PARTITION_BY_CHOICES
                                     , required=True)
            parser.add_argument('--partition-interval'
                                     , help='Partition creation interval – 1 - 24h , 1-31d, 1-12M, 1-Ny'
                                     , choices=TIME_CHOICES
                                     , metavar='partition_interval'
                                     , default='1h')
            parser.add_argument('--real-time-window'
                                     , help='how many hours or days to keep in the real time db - d , w , m'
                                     , choices=TIME_CHOICES
                                     , metavar='real_time_window'
                                     , default='1d')
            parser.add_argument('--historical-retention'
                                     , help='how many hours or days to keep in the hitorical data - d , w , m'
                                     , choices=TIME_CHOICES
                                     , metavar='historical_retention'
                                     , default='1w')
            parser.add_argument('--real-time-table-name'
                                     , help='kv table name'
                                     , metavar='real_time_table_name'
                                     , required=True)
            parser.add_argument('--config'
                                      , help='config path'
                                      , metavar="config_path"
                                      , required=False
                                      )

            return parser
        except Exception as e:
            print(e)
            self.logger.error(e)

    def parse_args(self):
        args = self.parser.parse_args()
        self.logger.info(args)
        return args











