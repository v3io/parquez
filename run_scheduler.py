from mlrun import get_or_create_ctx
from config.app_conf import AppConf
from core.params import Params
from core.cron_tab import CronTab
import re
from mlrun import code_to_function


def create_cron_string(partition_interval):
    PARTITION_BY_RE = r"([0-9]+)([a-zA-Z]+)"
    m = re.match(PARTITION_BY_RE, partition_interval)
    if m.group(2) == 'm':
        result = "*/" + m.group(1) + " * * * * "
    if m.group(2) == 'h':
        result = "0 " + "*/" + m.group(1) + " * * * "
    if m.group(2) == 'd':
        if m.group(1) == 1:
            result = "0 0 " + "* * * "
        else:
            result = "0 0 " + "*/" + m.group(1) + " * * "
    if m.group(2) == 'M':
        result = "0 0 0" + "*/" + m.group(1) + " * "
    if m.group(2) == 'DW':
        result = "0 0 0 0 " + "*/" + m.group(1)
    return result


def main(context):
    context.logger.info("")
    params = Params()
    params.set_params_from_context(context)
    context.logger.info("generating cronJob")
    cron_str = create_cron_string(params.partition_interval)
    fn = code_to_function(name="run_interval", filename="run_parquez_interval.py")
    fn.run(artifact_path='/User/artifacts', schedule=cron_str)

if __name__ == '__main__':
    context = get_or_create_ctx('run scheduler')
    main(context)