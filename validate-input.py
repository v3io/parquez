from mlrun import get_or_create_ctx
from core.input_parser import InputParser
from core.parquet_table import ParquetTable
from core.cron_tab import CronTab
from config.app_conf import AppConf
from core.kv_table import KVTable
from core.kv_view import KVView
from core.presto_client import PrestoClient
from utils.utils import Utils
from core.k8s_client import K8SClient

CONFIG_PATH = 'User/parquez/config/parquez.ini'

def main(context
         ,view_name="view_name"
         ,partition_by='h'
         ,partition_interval='1h'
         ,real_time_window='1d'
         ,historical_retention='7d'
         ,real_time_table_name="faker"
         ,config_path=CONFIG_PATH):
    context.logger.info("Starting to Parquezzzzzzzz")

    parser = InputParser(context.logger)
    args = parser.parse_args(["--view-name", view_name
                                 ,"--partition-by", partition_by
                                 ,"--partition-interval", partition_interval
                                 , "--real-time-window",real_time_window
                                 , "--historical-retention", historical_retention
                                 , "--real-time-table-name", real_time_table_name
                                 ,"--config", config_path
                              ])
    context.logger.info("input parsed")


if __name__ == '__main__':
    context = get_or_create_ctx('validate-input')
    import sys
    pb = context.get_param('partition_by')
    context.logger.info('Number of arguments: {}'.format(len(sys.argv)))
    context.logger.info('Argument(s) passed: {}'.format(str(sys.argv)))
    context.logger.info(pb)
    
    main(context)
