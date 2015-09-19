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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.io.Files;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Volume;

/**
 * Loads configuration file for executors from a JSON file
 * returns a map that can be used to dynamically choose executors
 */
public final class ExecutorSettingsLoader {

  private ExecutorSettingsLoader()  {
    // Utility class
  }

  public static ImmutableMap<String, ExecutorSettings> load(File configFile) {

    Map<String, ExecutorConfig> executorSettings = ImmutableMap.of();

    String configContents;
    try {
      configContents = Files.toString(configFile, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    mapper.registerModule(new ProtobufModule());

    try {
      executorSettings = mapper.readValue(configContents, new TypeReference<Map<String, ExecutorConfig>>() {});
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    for (ExecutorConfig config : executorSettings.values()) {
      System.out.println("Executor: " + config.command.build());
      System.out.println("Volumes: " + config.volumeMounts);
      System.out.println("Map: " + config.config);
    }

    Map<String, ExecutorSettings> map = Maps.transformEntries(executorSettings, FROM_CONFIG);
    return ImmutableMap.<String, ExecutorSettings>copyOf(map);
  }


  public static class ExecutorConfig {
    public CommandInfo.Builder command;
    public List<Volume> volumeMounts = ImmutableList.of();
    public Overhead overhead;
    public Map<String, String> config = ImmutableMap.of();

    static private class Overhead {
      public double cpus = 0;
      public long ram = 0;
      public long disk = 0;
      public int ports = 0;
    }
  }

  private static final EntryTransformer<String, ExecutorConfig, ExecutorSettings> FROM_CONFIG =
      new EntryTransformer<String, ExecutorConfig, ExecutorSettings>() {
        @Override
        public ExecutorSettings transformEntry(String key, ExecutorConfig config) {

          return ExecutorSettings.newBuilder()
              .setExecutorName(key)
              .setCommandInfo(config.command)
              .setGlobalContainerMounts(config.volumeMounts)
              .setExecutorOverhead(new ResourceSlot(
                  config.overhead.cpus,
                  Amount.of(config.overhead.ram, Data.MB),
                  Amount.of(config.overhead.disk, Data.MB),
                  config.overhead.ports))
              .setThermosObserverRoot(config.config.get("thermosObserverRoot"))
              .setConfig(config.config)
              .build();
        }
      };

  public static void main(String...args) {

    ExecutorConfig test = new ExecutorConfig();
    test.command = CommandInfo.newBuilder();
    test.volumeMounts = new ArrayList<Volume>();
    test.volumeMounts.add(Volume.newBuilder().setHostPath("host").setContainerPath("container").setMode(Volume.Mode.RW).build());
    //test.overhead = new ResourceSlot(23.0, Amount.of(12L, Data.MB), Amount.of(230L, Data.MB), 20);
    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    mapper.registerModule(new ProtobufModule());

    try {
      mapper.writeValue(System.out, test);
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("hello wrodl");

  }
}
