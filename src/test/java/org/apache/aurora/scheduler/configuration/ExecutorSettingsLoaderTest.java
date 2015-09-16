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
package org.apache.aurora.scheduler.configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.app.VolumeParser;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Volume;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExecutorSettingsLoaderTest {
  private static final String SINGLE_EXECUTOR_RESOURCE = "single-executor-example.json";
  private static final String MULTI_EXECUTOR_RESOURCE = "multiple-executor-example.json";
  private static final String  THERMOS_NO_OBSERVER_RESOURCE
      = "executor-settings-thermos-no-observer.json";
  private static final String NO_VALUE_URI = "no-value-URI.json";
  private static final String NONEXISTENT_RESOURCE = "executor-settings-nonexistent.json";
  private static final Map<String, String> CONFIG = new HashMap<String, String>();
  static {
    CONFIG.put("thermosObserverRoot", "/var/run/thermos");
  }

  private static final VolumeParser VOLUME_PARSER = new VolumeParser();
  private static final ExecutorSettings THERMOS_EXECUTOR = ExecutorSettings.newBuilder()
      .setExecutorName("AuroraExecutor")
      .setExecutorCommand(Protos.CommandInfo.newBuilder()
          .setValue("thermos_executor.pex")
          .addArguments("--announcer-enable")
          .addArguments("--announcer-ensemble")
          .addArguments("localhost:2181")
          .addUris(
              URI.newBuilder()
                  .setValue("/home/vagrant/aurora/dist/thermos_executor.pex")
                  .setExecutable(true)
                  .setExtract(false)
                  .setCache(false).build()))
      .setGlobalContainerMounts(ImmutableList.of(
          Volume.newBuilder()
              .setHostPath("host")
              .setContainerPath("container")
              .setMode(Protos.Volume.Mode.RW).build(),
          Volume.newBuilder()
              .setHostPath("host2")
              .setContainerPath("container2")
              .setMode(Protos.Volume.Mode.RO).build()))
      .setExecutorOverhead(
          new ResourceSlot(0.25, Amount.of(128L, Data.MB), Amount.of(0L, Data.MB), 0))
      .setThermosObserverRoot("/var/run/thermos")
      .setConfig(CONFIG).build();

  private static final ExecutorSettings COMMAND_EXECUTOR = ExecutorSettings.newBuilder()
      .setExecutorName("CommandExecutor")
      .setExecutorCommand(Protos.CommandInfo.newBuilder()
          .setValue("echo")
          .addArguments("'Hello World from Aurora!'"))
      .setGlobalContainerMounts(ImmutableList.of())
      .setExecutorOverhead(
          new ResourceSlot(0.25, Amount.of(128L, Data.MB), Amount.of(0L, Data.MB), 0))
      .setThermosObserverRoot("").build();

  @Test
  public void parseMultiple() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    Map<String, ExecutorSettings> test = ExecutorSettingsLoader.load(
        new File(getClass().getResource(MULTI_EXECUTOR_RESOURCE).getFile()));

    assertEquals(THERMOS_EXECUTOR, test.get(THERMOS_EXECUTOR.getExecutorName()));
    assertEquals(COMMAND_EXECUTOR, test.get(COMMAND_EXECUTOR.getExecutorName()));
  }

  @Test
  public void parseSingle() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    Map<String, ExecutorSettings> test = ExecutorSettingsLoader.load(
        new File(getClass().getResource(SINGLE_EXECUTOR_RESOURCE).getFile()));

  System.out.println(test.get(THERMOS_EXECUTOR.getExecutorName()));
    System.out.println("VALUE " + test.get(THERMOS_EXECUTOR.getExecutorName()).getExecutorCommand().getUrisList());
    assertEquals(THERMOS_EXECUTOR, test.get(THERMOS_EXECUTOR.getExecutorName()));
    //assertNotNull(test);
  }

  @Test(expected = ExecutorSettingsLoader.ExecutorSettingsConfigException.class)
  public void testThermosNoObserver()
      throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(
        new File(getClass().getResource(THERMOS_NO_OBSERVER_RESOURCE).getFile()));
  }

  @Test(expected = ExecutorSettingsLoader.ExecutorSettingsConfigException.class)
  public void testThermosNoValueURI()
      throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(
        new File(getClass().getResource(NO_VALUE_URI).getFile()));
  }

  @Test(expected = ExecutorSettingsLoader.ExecutorSettingsConfigException.class)
  public void testNonExistentFile() throws ExecutorSettingsLoader.ExecutorSettingsConfigException {
    ExecutorSettingsLoader.load(new File(NONEXISTENT_RESOURCE));
  }
}
