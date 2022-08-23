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
from kubernetes import config, client
from kubernetes.stream import stream


class K8SClient(object):

    def __init__(self, logger, namespace='default-tenant', config_file=None):
        self.namespace = namespace
        self.logger = logger
        self._init_k8s_config(config_file)
        self.v1api = client.CoreV1Api()

    def _init_k8s_config(self, config_file):
        try:
            config.load_incluster_config()
            self.logger.info('using in-cluster config.')
        except Exception:
            try:
                config.load_kube_config(config_file)
                self.logger.info('using local kubernetes config.')
            except Exception:
                raise RuntimeError(
                    'cannot find local kubernetes config file,'
                    ' place it in ~/.kube/config or specify it in '
                    'KUBECONFIG env var')

    def get_shell_pod_name(self, pod_name='shell'):
        shell_pod = self.v1api.list_namespaced_pod(namespace=self.namespace)
        for i in shell_pod.items:
            if pod_name in i.metadata.name:
                self.logger.info("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
                shell_name = i.metadata.name
                break
        return shell_name

    def exec_shell_cmd(self, cmd, shell_pod_name='shell'):
        shell_name = self.get_shell_pod_name(shell_pod_name)
        # Calling exec and waiting for response
        exec_command = [
            '/bin/bash',
            '-c',
            cmd]
        resp = stream(self.v1api.connect_get_namespaced_pod_exec,
                      shell_name,
                      self.namespace,
                      command=exec_command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)
        self.logger.info("Response: " + resp)
        return resp


