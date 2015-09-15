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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.io.Files;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

import org.apache.aurora.scheduler.app.VolumeParser;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Volume;

/**
 * Loads configuration file for executors from a JSON file
 * returns a map that can be used to dynamically choose executors
 */
public final class ExecutorSettingsLoader {
  private static final String AURORA_EXECUTOR = "AuroraExecutor";

  private ExecutorSettingsLoader()  {
    // Utility class
  }

  public static class ExecutorSettingsConfigException extends Exception {
    public ExecutorSettingsConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }


  public static ImmutableMap<String, ExecutorSettings> load(File configFile)
      throws ExecutorSettingsConfigException {

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
      System.out.println("Executor: " + config.executor.setExecutorId(Protos.ExecutorID.newBuilder().setValue("5")).build());
      System.out.println("Volumes: " + FluentIterable.from(config.volumeMounts).transform(Volume.Builder::build).toList());
      System.out.println("Map: " + config.config);
    }

    Map<String, ExecutorSettings> map = Maps.transformEntries(executorSettings, FROM_CONFIG);
    return ImmutableMap.<String, ExecutorSettings>copyOf(map);

   // return ImmutableMap.of();
  }


  public static class ExecutorConfig {
    public ExecutorInfo.Builder executor;
    public List<Protos.Volume.Builder> volumeMounts;
    public Map<String, String> config;
  }

  private static final EntryTransformer<String, ExecutorConfig, ExecutorSettings> FROM_CONFIG =
      new EntryTransformer<String, ExecutorConfig, ExecutorSettings>() {
        @Override
        public ExecutorSettings transformEntry(String key, ExecutorConfig config) {

          return ExecutorSettings.newBuilder()
              .setExecutorName(key)
              .setExecutorCommand(config.executor.getCommandBuilder())
              //.setGlobalContainerMounts(config.volumeMounts)
              .setExecutorResources(config.executor.getCommand().getUrisList())
              .setThermosObserverRoot(config.config.get("thermosObserverRoot"))
              //.setExecutorOverhead(config.executor.getResources(0).)
              .setConfig(config.config)
              .build();
        }
      };
}
