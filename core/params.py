
class Params(object):
    def __init__(self
                 , view_name="view_name"
                 , partition_by='h'
                 , partition_interval='1h'
                 , real_time_window='1d'
                 , historical_retention='7d'
                 , real_time_table_name="real_time_table_name"
                 , config_path='parquez.ini'
                 , project_path='~/parquez'
                 , shell_pod_name ='~/parquez'
                 ):
        self.view_name = view_name
        self.partition_by = partition_by
        self.partition_interval = partition_interval
        self.real_time_window = real_time_window
        self.historical_retention = historical_retention
        self.real_time_table_name = real_time_table_name
        self.config_path = config_path
        self.project_path = project_path
        self.shell_pod_name = shell_pod_name

    def set_params_from_context(self, context):
        self.view_name = context.parameters['view_name']
        self.partition_by = context.parameters['partition_by']
        self.partition_interval = context.parameters['partition_interval']
        self.real_time_window = context.parameters['real_time_window']
        self.historical_retention = context.parameters['historical_retention']
        self.real_time_table_name = context.parameters['real_time_table_name']
        self.config_path = context.parameters['config_path']
        self.project_path = context.parameters['project_path']
        self.shell_pod_name = context.parameters['shell_pod_name']

