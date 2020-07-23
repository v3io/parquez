from utils.logger import Logger
from core.k8s import K8SClient
import sys


def delete_partition(source=None, target=None):
    logger = Logger()
    if source is None:
        source = sys.argv[1]
    logger.info("source is {}".format(source))
    if target is None:
        target = sys.argv[2]
    logger.info("target is {}".format(target))
    cmd = "hdfs dfs -count {} | awk '{{print $2}}'".format(target)
    logger.info(cmd)
    cli = K8SClient(logger)
    resp = cli.exec_shell_cmd(cmd)
    splited = resp.split('\n')
    if len(splited) > 1:
        val = splited[1]
        if not ("No such file or directory" in val):
            if int(val) > 0:
                delete_cmd = "hdfs dfs -rm -r {} ".format(source)
                cli.exec_shell_cmd(delete_cmd)
                logger.info("partition {} exists".format(target))
                return
    logger.error("partition {} not deleted".format(source))


if __name__ == "__main__":
    delete_partition()
