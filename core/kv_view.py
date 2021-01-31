from datetime import datetime, timedelta
import re
from pyhive import presto
from core.params import Params
from config.app_conf import AppConf
from core.presto_client import PrestoClient

PARTITION_INTERVAL_RE = r"([0-9]+)([a-zA-Z]+)"


# TODO: Add verification that hive table created (handle Trying to send on a closed client exception


class KVView(object):
    def __init__(self, logger, params: Params, conf: AppConf, presto_client: PrestoClient):
        self.logger = logger
        self.params = params
        self.conf = conf
        self.presto_client = presto_client
        # self.name = params.real_time_table_name + "_view"
        # self.real_time_window = params.real_time_window
        # self.conf = conf
        # self.uri = conf.presto_uri
        # self.cursor = None

    def parse_real_time_window(self):
        now = datetime.utcnow()
        part = re.match(PARTITION_INTERVAL_RE, self.params.real_time_window).group(2)
        val = int(re.match(PARTITION_INTERVAL_RE, self.params.real_time_window).group(1))
        self.logger.debug("generate time window".format(part))
        if part == 'd':
            window_time = now - timedelta(days=val - 1)
        if part == 'h':
            window_time = now - timedelta(hours=val - 1)
        self.logger.info("window Time " + str(window_time))
        return window_time

    def generate_where_clause(self):
        window_time = self.parse_real_time_window()
        part = re.match(PARTITION_INTERVAL_RE, self.params.real_time_window).group(2)
        self.logger.debug("generate_partition_by {0}".format(part))
        condition = ''
        if part == 'y':
            condition += "year>="+str(window_time.year)
        if part == 'm':
            condition += "year>="+str(window_time.year)+" AND month>="+str(window_time.month)
        if part == 'd':
            condition += "year>=" + str(window_time.year) + " AND month>=" + str(window_time.month)+" AND day>=" \
                         + str(window_time.day)
        if part == 'h':
            condition += "year>=" + str(window_time.year) + " AND month>=" + str(window_time.month) + " AND day>=" + \
                        str(window_time.day) + " AND hour>="+str(window_time.hour)
        clause = " WHERE "+condition
        return clause

    def create_view_prefix(self):
        hive_prefix = "hive." + self.conf.hive_schema + "."
        v3io_prefix = "v3io." + self.conf.v3io_container + "."
        prefix = "CREATE OR REPLACE VIEW " + hive_prefix + self.params.real_time_table_name + \
                 "_view AS SELECT * FROM " + v3io_prefix + self.params.real_time_table_name
        return prefix

    def create_view(self, command):
        self.logger.debug("Create view command : " + command)
        self.presto_client.connect()
        self.presto_client.execute_command(command)
        self.logger.info(self.presto_client.fetch_results())
        self.presto_client.disconnect()

    def generate_crete_view_script(self):
        try:
            self.logger.debug("generating kv view script")
            prefix = self.create_view_prefix()
            clause = self.generate_where_clause()
            script = prefix + clause
            self.logger.info("create kv view script {}".format(script))
            self.create_view(script)
        except Exception as e:
            self.logger.error(e)
            raise

    def drop_view(self):
        try:
            self.logger.info("dropping kv view ")
            hive_prefix = "hive." + self.conf.hive_schema + "."
            command = "DROP VIEW IF EXISTS " + hive_prefix + self.params.real_time_table_name + "_view"
            self.connect()
            self.execute_command(command)
            self.logger.info(self.cursor.fetchall())
            self.disconnect()
        except Exception as e:
            self.logger.error(e)
            raise



