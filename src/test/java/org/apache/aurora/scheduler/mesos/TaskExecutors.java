/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.mesos;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.app.VolumeParser;

/**
 * Utility class to contain constants related to setting up executor settings.
 */
public final class TaskExecutors {

  private TaskExecutors() {
    // Utility class.
  }

  public static final ImmutableMap<String, ExecutorSettings> TASK_EXECUTORS;
  private static final String EXECUTOR_WRAPPER_PATH = "/fake/executor_wrapper.sh";
  private static final String EXECUTOR_PATH = "/fake/executor.pex";

  public static final ExecutorSettings NO_OVERHEAD_EXECUTOR_SETTINGS =
      ExecutorSettings.newBuilder()
          .setExecutorName("no-overhead")
          .setExecutorPath(EXECUTOR_PATH)
          .setThermosObserverRoot("/var/run/thermos")
          .build();

  public static final ExecutorSettings SOME_OVERHEAD_EXECUTOR_SETTINGS =
      ExecutorSettings.newBuilder()
          .setExecutorName("some-overhead")
          .setExecutorPath(EXECUTOR_PATH)
          .setThermosObserverRoot("/var/run/thermos")
          .setExecutorOverhead(
              new ResourceSlot(0.01, Amount.of(256L, Data.MB), Amount.of(0L, Data.MB), 0))
          .build();

  public static final ExecutorSettings FAKE_MESOS_COMMAND_EXECUTOR_SETTINGS =
      ExecutorSettings.newBuilder()
          .setExecutorName("mesos-command")
          .setExecutorResources(Arrays.<String>asList("/path/to/resource"))
          .setExecutorFlags(Optional.<String>fromNullable(""))
          .setGlobalContainerMounts(
              Arrays.<Volume>asList(new VolumeParser().doParse("/host:/container1:ro")))
          .setExecutorOverhead(
              new ResourceSlot(1.25, Amount.of(128L, Data.MB), Amount.of(0L, Data.MB), 0))
          .build();

  public static final ExecutorSettings FAKE_DOCKER_EXECUTOR_SETTINGS =
      ExecutorSettings.newBuilder()
          .setExecutorName("docker")
          .setExecutorPath(EXECUTOR_WRAPPER_PATH)
          .setExecutorResources(ImmutableList.of(SOME_OVERHEAD_EXECUTOR_SETTINGS.getExecutorPath()))
          .setThermosObserverRoot("/var/run/thermos")
          .setExecutorOverhead(SOME_OVERHEAD_EXECUTOR_SETTINGS.getExecutorOverhead())
          .setGlobalContainerMounts(ImmutableList.of(
              new Volume("/container", "/host", Mode.RO)))
          .build();

  public static final ExecutorSettings WRAPPER_TEST_EXECUTOR_SETTINGS =
      ExecutorSettings.newBuilder()
          .setExecutorName("wrapper-test")
          .setExecutorPath(EXECUTOR_WRAPPER_PATH)
          .setExecutorResources(ImmutableList.of(NO_OVERHEAD_EXECUTOR_SETTINGS.getExecutorPath()))
          .setThermosObserverRoot("/var/run/thermos")
          .setExecutorOverhead(NO_OVERHEAD_EXECUTOR_SETTINGS.getExecutorOverhead())
          .build();

  static {
    Map<String, ExecutorSettings> temp = new HashMap<String, ExecutorSettings>();
    temp.put(NO_OVERHEAD_EXECUTOR_SETTINGS.getExecutorName(), NO_OVERHEAD_EXECUTOR_SETTINGS);
    temp.put(SOME_OVERHEAD_EXECUTOR_SETTINGS.getExecutorName(), SOME_OVERHEAD_EXECUTOR_SETTINGS);
    temp.put(FAKE_MESOS_COMMAND_EXECUTOR_SETTINGS.getExecutorName(),
        FAKE_MESOS_COMMAND_EXECUTOR_SETTINGS);
    temp.put(FAKE_DOCKER_EXECUTOR_SETTINGS.getExecutorName(), FAKE_DOCKER_EXECUTOR_SETTINGS);
    temp.put(WRAPPER_TEST_EXECUTOR_SETTINGS.getExecutorName(), WRAPPER_TEST_EXECUTOR_SETTINGS);

    TASK_EXECUTORS = ImmutableMap.<String, ExecutorSettings>copyOf(temp);
  }
}
