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

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.ResourceSlot.MIN_EXECUTOR_RESOURCES;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.SOME_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.WRAPPER_TEST_EXECUTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosTaskFactoryImplTest {

  private static final String EXECUTOR_WRAPPER_PATH = "/fake/executor_wrapper.sh";
  private static final TaskConfig TASK_CONFIG = new TaskConfig()
          .setJob(new JobKey("role", "environment", "job-name"))
          .setOwner(new Identity("role", "user"))
          .setEnvironment("environment")
          .setJobName("job-name")
          .setDiskMb(10)
          .setRamMb(100)
          .setNumCpus(5)
          .setContainer(Container.mesos(new MesosContainer()))
          .setRequestedPorts(ImmutableSet.of("http"));
  private static final AssignedTask ASSIGNED_TASK_BASE =
      new AssignedTask()
      .setInstanceId(2)
      .setTaskId("task-id")
      .setAssignedPorts(ImmutableMap.of("http", 80));

  private static final IAssignedTask NO_OVERHEAD_TASK = IAssignedTask.build(ASSIGNED_TASK_BASE
      .setTask(TASK_CONFIG.setExecutorConfig(new ExecutorConfig("no-overhead", "config"))));
  private static final IAssignedTask SOME_OVERHEAD_TASK = IAssignedTask.build(ASSIGNED_TASK_BASE
      .setTask(TASK_CONFIG.setExecutorConfig(new ExecutorConfig("some-overhead", "config"))));
  private static final IAssignedTask WRAPPER_TEST_TASK = IAssignedTask.build(ASSIGNED_TASK_BASE
      .setTask(TASK_CONFIG.setExecutorConfig(new ExecutorConfig("wrapper-test", "config"))));
  private static final IAssignedTask TASK_WITH_DOCKER = IAssignedTask.build(
      SOME_OVERHEAD_TASK.newBuilder()
      .setTask(
          new TaskConfig(SOME_OVERHEAD_TASK.getTask().newBuilder())
              .setExecutorConfig(new ExecutorConfig("docker", "config"))
              .setContainer(Container.docker(
                  new DockerContainer("testimage")))));
  private static final IAssignedTask TASK_WITH_DOCKER_PARAMS = IAssignedTask.build(
      SOME_OVERHEAD_TASK.newBuilder()
      .setTask(
          new TaskConfig(SOME_OVERHEAD_TASK.getTask().newBuilder())
              .setContainer(Container.docker(
                  new DockerContainer("testimage").setParameters(
                      ImmutableList.of(new DockerParameter("label", "testparameter")))))));

  private static final SlaveID SLAVE = SlaveID.newBuilder().setValue("slave-id").build();

  private MesosTaskFactory taskFactory;
  private Map<String, ExecutorSettings> config;

  private static final ExecutorInfo NO_OVERHEAD_EXECUTOR_INFO = ExecutorInfo.newBuilder()
      .setExecutorId(
          Protos.ExecutorID.newBuilder().setValue(
              NO_OVERHEAD_EXECUTOR.getExecutorName() + "-" + NO_OVERHEAD_TASK.getTaskId()).build())
      .setName(NO_OVERHEAD_EXECUTOR.getExecutorName())
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(
          NO_OVERHEAD_TASK.getTask(),
          NO_OVERHEAD_TASK.getInstanceId()))
      .addAllResources(MesosTaskFactoryImpl.RESOURCES_EPSILON.toResourceList())
      .setCommand(CommandInfo.newBuilder()
          .setValue("./executor.pex")
          .addUris(URI.newBuilder().setValue(NO_OVERHEAD_EXECUTOR.getExecutorPath())
              .setExecutable(true)))
      .build();

  private static final ExecutorInfo WRAPPER_TEST_EXECUTOR_INFO = ExecutorInfo.newBuilder()
      .setExecutorId(
          Protos.ExecutorID.newBuilder().setValue(
              WRAPPER_TEST_EXECUTOR.getExecutorName()
                  + "-"
                  + WRAPPER_TEST_TASK.getTaskId()).build())
      .setName(WRAPPER_TEST_EXECUTOR.getExecutorName())
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(
          WRAPPER_TEST_TASK.getTask(),
          WRAPPER_TEST_TASK.getInstanceId()))
      .addAllResources(MesosTaskFactoryImpl.RESOURCES_EPSILON.toResourceList())
      .setCommand(CommandInfo.newBuilder()
          .setValue("./executor.pex")
          .addUris(URI.newBuilder().setValue(WRAPPER_TEST_EXECUTOR.getExecutorPath())
              .setExecutable(true)))
      .build();

  private static final ExecutorInfo SOME_OVERHEAD_EXECUTOR_INFO = ExecutorInfo.newBuilder()
      .setExecutorId(
          Protos.ExecutorID.newBuilder().setValue(
              SOME_OVERHEAD_EXECUTOR.getExecutorName()
                  + "-"
                  + SOME_OVERHEAD_TASK.getTaskId()).build())
      .setName(SOME_OVERHEAD_EXECUTOR.getExecutorName())
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(
          SOME_OVERHEAD_TASK.getTask(),
          SOME_OVERHEAD_TASK.getInstanceId()))
      .addAllResources(MesosTaskFactoryImpl.RESOURCES_EPSILON.toResourceList())
      .setCommand(CommandInfo.newBuilder()
          .setValue("./executor.pex")
          .addUris(URI.newBuilder().setValue(SOME_OVERHEAD_EXECUTOR.getExecutorPath())
              .setExecutable(true)))
      .build();

  private static final ExecutorInfo NO_OVERHEAD_EXECUTOR_INFO_WITH_WRAPPER =
      ExecutorInfo.newBuilder(WRAPPER_TEST_EXECUTOR_INFO)
          .setCommand(CommandInfo.newBuilder()
              .setValue("./executor_wrapper.sh")
              .addUris(URI.newBuilder().setValue(NO_OVERHEAD_EXECUTOR.getExecutorPath())
                  .setExecutable(true))
              .addUris(URI.newBuilder().setValue(EXECUTOR_WRAPPER_PATH).setExecutable(true)))
          .build();

  private static final ExecutorInfo SOME_OVERHEAD_EXECUTOR_INFO_WITH_WRAPPER =
      ExecutorInfo.newBuilder(SOME_OVERHEAD_EXECUTOR_INFO)
          .setName("wrapper-test")
          .setCommand(CommandInfo.newBuilder()
              .setValue("./executor_wrapper.sh")
              .addUris(URI.newBuilder().setValue(NO_OVERHEAD_EXECUTOR.getExecutorPath())
                  .setExecutable(true))
              .addUris(URI.newBuilder().setValue(EXECUTOR_WRAPPER_PATH).setExecutable(true)))
          .build();
  @Before
  public void setUp() {
    config = TaskExecutors.TASK_EXECUTORS;
  }

  @Test
  public void testExecutorInfoUnchanged() {
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo task = taskFactory.createFrom(SOME_OVERHEAD_TASK, SLAVE);
    assertEquals(SOME_OVERHEAD_EXECUTOR_INFO, task.getExecutor());
    checkTaskResources(SOME_OVERHEAD_TASK.getTask(), task);
  }

  @Test
  public void testCreateFromPortsUnset() {
    taskFactory = new MesosTaskFactoryImpl(config);
    AssignedTask assignedTask = SOME_OVERHEAD_TASK.newBuilder();

    assignedTask.getTask().unsetRequestedPorts();
    assignedTask.unsetAssignedPorts();
    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(assignedTask), SLAVE);
    checkTaskResources(ITaskConfig.build(assignedTask.getTask()), task);
  }

  @Test
  public void testExecutorInfoNoOverhead() {
    // Here the ram required for the executor is greater than the sum of task resources
    // + executor overhead. We need to ensure we allocate a non-zero amount of ram in this case.
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo task = taskFactory.createFrom(NO_OVERHEAD_TASK, SLAVE);
    assertEquals(NO_OVERHEAD_EXECUTOR_INFO, task.getExecutor());

    // Simulate the upsizing needed for the task to meet the minimum thermos requirements.
    TaskConfig dummyTask = NO_OVERHEAD_TASK.getTask().newBuilder()
        .setRamMb(ResourceSlot.MIN_EXECUTOR_RESOURCES.getRam().as(Data.MB));
    checkTaskResources(ITaskConfig.build(dummyTask), task);
  }

  @Test
  public void testSmallTaskUpsizing() {
    // A very small task should be upsized to support the minimum resources required by the
    // executor.

    taskFactory = new MesosTaskFactoryImpl(config);

    AssignedTask builder = NO_OVERHEAD_TASK.newBuilder();
    builder.getTask()
        .setNumCpus(0.001)
        .setRamMb(1)
        .setDiskMb(0)
        .setRequestedPorts(ImmutableSet.of());
    IAssignedTask assignedTask =
        IAssignedTask.build(builder.setAssignedPorts(ImmutableMap.of()));

    assertEquals(
        MIN_EXECUTOR_RESOURCES,
        getTotalTaskResources(taskFactory.createFrom(assignedTask, SLAVE)));
  }

  private void checkTaskResources(ITaskConfig task, TaskInfo taskInfo) {
    assertEquals(
        ResourceSlot.sum(
            Resources.from(task),
            config.get(taskInfo.getExecutor().getName()).getExecutorOverhead()),
            getTotalTaskResources(taskInfo));
  }

  private TaskInfo getDockerTaskInfo() {
    return getDockerTaskInfo(TASK_WITH_DOCKER);
  }

  private TaskInfo getDockerTaskInfo(IAssignedTask task) {
    taskFactory = new MesosTaskFactoryImpl(config);
    return taskFactory.createFrom(task, SLAVE);
  }

  @Test
  public void testDockerContainer() {
    DockerInfo docker = getDockerTaskInfo().getExecutor().getContainer().getDocker();
    assertEquals("testimage", docker.getImage());
    assertTrue(docker.getParametersList().isEmpty());
  }

  @Test
  public void testDockerContainerWithParameters() {
    DockerInfo docker = getDockerTaskInfo(TASK_WITH_DOCKER_PARAMS).getExecutor().getContainer()
            .getDocker();
    Parameter parameters = Parameter.newBuilder().setKey("label").setValue("testparameter").build();
    assertEquals(ImmutableList.of(parameters), docker.getParametersList());
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidExecutorSettings() {
    ExecutorSettings.newBuilder()
        .setExecutorName("")
        .setExecutorPath(null)
        .setThermosObserverRoot("")
        .build();
  }

  @Test
  public void testExecutorAndWrapper() {
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo taskInfo = taskFactory.createFrom(WRAPPER_TEST_TASK, SLAVE);
    assertEquals(NO_OVERHEAD_EXECUTOR_INFO_WITH_WRAPPER, taskInfo.getExecutor());
  }

  @Test
  public void testGlobalMounts() {
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo taskInfo = taskFactory.createFrom(TASK_WITH_DOCKER, SLAVE);
    Protos.Volume expected = Protos.Volume.newBuilder()
        .setHostPath("/host")
        .setContainerPath("/container")
        .setMode(Protos.Volume.Mode.RO)
        .build();
    assertTrue(taskInfo.getExecutor().getContainer().getVolumesList().contains(expected));
  }

  private static Resources getTotalTaskResources(TaskInfo task) {
    Resources taskResources = Resources.from(task.getResourcesList());
    Resources executorResources = Resources.from(task.getExecutor().getResourcesList());
    return ResourceSlot.sum(taskResources, executorResources);
  }
}
