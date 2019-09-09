import os
import re

SHELL_PATH = '~/parquez/sh/'
PARTITION_BY_RE = r"([0-9]+)([a-zA-Z]+)"


def window_parser(window_type):
    m = re.match(PARTITION_BY_RE, window_type)
    result = ""
    if m.group(2) == 'm':
        result = m.group(1) + " minutes"
    if m.group(2) == 'h':
        result = m.group(1) + " hours"
    if m.group(2) == 'd':
        result = m.group(1) + " days"
    if m.group(2) == 'M':
        result = m.group(1) + " months"
    return result


class Crontab:

    def __init__(self, logger, conf, kv_table_name, partition_interval, key_value_window, historical_retention
                 , partition_by):

        self.logger = logger
        self.conf = conf
        self.kv_table_name = kv_table_name
        self.partition_interval = partition_interval
        self.key_value_window = key_value_window
        self.historical_retention = historical_retention
        self.partition_by = partition_by

    def partition_interval_parser(self):
        m = re.match(PARTITION_BY_RE, self.partition_interval)
        #if m.group(2) == 'm':
            #result = "*/" + m.group(1) + " * * * * "
        if m.group(2) == 'h':
            result = "0 " + "*/" + m.group(1) + " * * * "
        if m.group(2) == 'd':
            if m.group(1) == 1:
                result = "0 0 " + "* * * "
            else:
                result = "0 0 " + "*/" + m.group(1) + " * * "
        if m.group(2) == 'M':
            result = "0 0 * " + "*/" + m.group(1) + " * "
        #if m.group(2) == 'y':
            #result = "0 0 0 0 " + "*/" + m.group(1)
        return result

    def create_cron_job(self):
        args2 = "'" + window_parser(self.key_value_window) + "'"
        args3 = "'" + window_parser(self.historical_retention) + "'"
        args4 = "'" + self.partition_by + "'"
        args5 = "'" + self.conf.v3io_container + "'"
        args6 = "'"+self.conf.hive_schema+"'"
        args7 = "'"+self.conf.compression+"'"
        args8 = "'" + self.conf.coalesce+"'"

        command = "\"" + self.partition_interval_parser() + SHELL_PATH + "parquetinizer.sh " + self.kv_table_name + " " +\
                  args2 + " " + args3 + " " + args4 + " " + args5+" " + args6+" "+args7+" "+args8+"\""
        self.logger.debug(command)
        os.system(SHELL_PATH + "parquetCronJob.sh " + command)

