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
import unittest

from mock import Mock

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.ttypes import (
    JobKey,
    JobUpdateRequest,
    JobUpdateSettings,
    Lock,
    LockKey,
    LockValidation,
    Response,
    ResponseCode,
    Result,
    TaskConfig
)


class TestJobUpdateApis(unittest.TestCase):
  """Job update APIs tests."""

  UPDATE_CONFIG = {
      'batch_size': 1,
      'restart_threshold': 50,
      'watch_secs': 50,
      'max_per_shard_failures': 2,
      'max_total_failures': 1,
      'rollback_on_failure': True,
      'wait_for_batch_completion': False,
  }

  @classmethod
  def create_blank_response(cls, code, msg):
    response = Mock(spec=Response)
    response.responseCode = code
    response.messageDEPRECATED = msg
    response.result = Mock(spec=Result)
    return response

  @classmethod
  def create_simple_success_response(cls):
    return cls.create_blank_response(ResponseCode.OK, 'OK')

  @classmethod
  def create_error_response(cls):
    return cls.create_blank_response(ResponseCode.ERROR, 'ERROR')

  @classmethod
  def mock_api(cls):
    api = AuroraClientAPI(Cluster(name="foo"))
    mock_proxy = Mock()
    api._scheduler_proxy = mock_proxy
    return api, mock_proxy

  @classmethod
  def create_update_settings(cls):
    return JobUpdateSettings(
        updateGroupSize=1,
        maxPerInstanceFailures=2,
        maxFailedInstances=1,
        maxWaitToInstanceRunningMs=50,
        minWaitInInstanceRunningMs=50,
        rollbackOnFailure=True)

  @classmethod
  def create_update_request(cls, task_config):
    return JobUpdateRequest(
        jobKey=JobKey(role="role", environment="env", name="name"),
        instanceCount=5,
        settings=cls.create_update_settings(),
        taskConfig=task_config)

  @classmethod
  def mock_job_config(cls, error=None):
    config = Mock(spec=AuroraConfig)
    mock_get = Mock()
    mock_get.get.return_value = cls.UPDATE_CONFIG
    if error:
      config.update_config.side_effect = error
    else:
      config.update_config.return_value = mock_get
    mock_task_config = Mock()
    mock_task_config.taskConfig = TaskConfig()
    config.job.return_value = mock_task_config
    config.role.return_value = "role"
    config.environment.return_value = "env"
    config.name.return_value = "name"
    config.instances.return_value = 5
    return config

  def test_acquire_lock(self):
    """Test successful job lock creation."""
    job_key = AuroraJobKey("foo", "role", "env", "name")
    api, mock_proxy = self.mock_api()
    mock_proxy.acquireLock.return_value = self.create_simple_success_response()
    api.acquire_job_lock(job_key)
    mock_proxy.acquireLock.assert_called_once_with(LockKey(job=job_key.to_thrift()))

  def test_acquire_lock_fails_validation(self):
    """Test acquire_job_lock fails with invalid job key."""
    api, mock_proxy = self.mock_api()
    self.assertRaises(AuroraClientAPI.TypeError, api.acquire_job_lock, "invalid job key")

  def test_release_lock(self):
    """Test successful lock release."""
    lock = Lock()
    api, mock_proxy = self.mock_api()
    mock_proxy.releaseLock.return_value = self.create_simple_success_response()
    api.release_job_lock(lock)
    mock_proxy.releaseLock.assert_called_once_with(lock, LockValidation.CHECKED)

  def test_release_lock_fails_validation(self):
    """Test release_job_lock fails with invalid lock."""
    api, mock_proxy = self.mock_api()
    self.assertRaises(AuroraClientAPI.Error, api.release_job_lock, "invalid lock")

  def test_start_job_update(self):
    """Test successful job update start."""
    api, mock_proxy = self.mock_api()
    task_config = TaskConfig()
    mock_proxy.startJobUpdate.return_value = self.create_simple_success_response()

    api.start_job_update(self.mock_job_config())
    mock_proxy.startJobUpdate.assert_called_once_with(self.create_update_request(task_config))

  def test_start_job_update_fails_parse_update_config(self):
    """Test start_job_update fails to parse invalid UpdateConfig."""
    api, mock_proxy = self.mock_api()

    self.assertRaises(
        AuroraClientAPI.UpdateConfigError,
        api.start_job_update,
        self.mock_job_config(error=ValueError()))
