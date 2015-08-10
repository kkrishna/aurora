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
from __future__ import print_function

import argparse
import logging
import sys

from twitter.common.log.formatters.plain import PlainFormatter

from apache.aurora.client.cli import CommandLine, ConfigurationPlugin
from apache.aurora.client.cli.options import CommandOption
from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.auth.auth_module_manager import register_auth_module
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.ttypes import (
  Constraint,
  Container,
  CronCollisionPolicy,
  DockerContainer,
  DockerParameter,
  ExecutorConfig,
  Identity,
  JobConfiguration,
  JobKey,
  LimitConstraint,
  MesosContainer,
  Metadata,
  TaskConfig,
  TaskConstraint,
  TaskQuery,
  ValueConstraint
)

class AuroraLogConfigurationPlugin(ConfigurationPlugin):
  """Plugin for configuring log level settings for the aurora client."""

  def get_options(self):
    return [
      CommandOption("--verbose",
                    "-v",
                    default=False,
                    action="store_true",
                    help=("Show verbose output"))
    ]

  def before_dispatch(self, raw_args):
    #TODO(zmanji): Consider raising the default log level to WARN.
    loglevel = logging.INFO
    for arg in raw_args:
      if arg == "--verbose" or arg == "-v":
        loglevel = logging.DEBUG

    logging.getLogger().setLevel(loglevel)
    handler = logging.StreamHandler()
    handler.setFormatter(PlainFormatter())
    logging.getLogger().addHandler(handler)
    return raw_args

  def before_execution(self, context):
    pass

  def after_execution(self, context, result_code):
    pass


class AuroraAuthConfigurationPlugin(ConfigurationPlugin):
  """Plugin for configuring aurora client authentication."""

  def get_options(self):
    return []

  def before_dispatch(self, raw_args):
    return raw_args

  def before_execution(self, context):
    try:
      from apache.aurora.common.auth.kerberos import KerberosAuthModule
      register_auth_module(KerberosAuthModule())
    except ImportError:
      # Use default auth implementation if kerberos is not available.
      pass

  def after_execution(self, context, result_code):
    pass


class AuroraCommandLine(CommandLine):
  """The CommandLine implementation for the Aurora client command line."""

  def __init__(self):
    super(AuroraCommandLine, self).__init__()
    self.register_plugin(AuroraLogConfigurationPlugin())
    self.register_plugin(AuroraAuthConfigurationPlugin())

  @property
  def name(self):
    return 'aurora'

  @classmethod
  def get_description(cls):
    return 'Aurora client command line'

  def register_nouns(self):
    super(AuroraCommandLine, self).register_nouns()
    from apache.aurora.client.cli.cron import CronNoun
    self.register_noun(CronNoun())
    from apache.aurora.client.cli.jobs import Job
    self.register_noun(Job())
    from apache.aurora.client.cli.config import ConfigNoun
    self.register_noun(ConfigNoun())
    from apache.aurora.client.cli.quota import Quota
    self.register_noun(Quota())
    from apache.aurora.client.cli.sla import Sla
    self.register_noun(Sla())
    from apache.aurora.client.cli.task import Task
    self.register_noun(Task())
    from apache.aurora.client.cli.update import Update
    self.register_noun(Update())


def parser():
  parser = argparse.ArgumentParser(description='Create custom thrift API calls for Apache Aurora')
  parser.add_argument('action', metavar='action', help='create or kill')
  parser.add_argument('jobName', metavar='job', help='job name')
  parser.add_argument('executorName', metavar='executor', help='name of the executor')
  parser.add_argument('data', metavar='data', help='data to be passed on to executor')

  args = parser.parse_args()

  return vars(args)

def proxy_main():

  parsedArgs = parser()

  clusterName = "devcluster"
  role = "www-data"
  environment = "devel"
  jobName = parsedArgs['jobName']
  user = "rdelvalle"
  isProduction = False
  isService = False
  maxTaskFailures = 4
  priority = 0
  email = "rdelvalle@paypal.com"

  numCpus = 1
  ramMb = 4
  diskMb = 8

  executorName = parsedArgs['executorName']
  executorData = parsedArgs['data']


  owner = Identity(role=role, user=user)

  key = JobKey(
    role=role,
    environment=environment,
    name=jobName)

  MB = 1024 * 1024
  task = TaskConfig()

  task.jobName = jobName
  task.environment = environment
  task.production = isProduction
  task.isService = isService
  task.maxTaskFailures = maxTaskFailures
  task.priority = priority
  task.contactEmail = email

  task.numCpus = numCpus
  task.ramMb = ramMb
  task.diskMb = diskMb

  task.job = key
  task.owner = owner
  task.requestedPorts = {}
  task.taskLinks = {}  # See AURORA-739
  task.constraints = {}
  #task.container = "mesos" = {}

  task.executorConfig = ExecutorConfig(name=executorName,data=executorData)

  config = JobConfiguration(
    key=key,
    owner=owner,
    cronSchedule= None,
    cronCollisionPolicy=0,
    taskConfig=task,
    instanceCount=1)

  taskQuery = TaskQuery()
  taskQuery.role = role
  taskQuery.environment = environment
  taskQuery.jobName = jobName


  cluster = Cluster(name=clusterName, zk="localhost", zk_port=2181, scheduler_zk_path="/aurora/scheduler", auth_mechanism="BASIC")

  try:
    client = AuroraClientAPI(cluster=cluster, user_agent="test")

    if 'kill' == parsedArgs['action']:
      result = client.scheduler_proxy().killTasks(taskQuery, None)
    else:
      result = client.scheduler_proxy().createJob(config, None)

    print(result)
  except TypeError:
    print('Type error')

if __name__ == '__main__':
  proxy_main()
