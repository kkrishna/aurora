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
package org.apache.aurora.scheduler.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;

/**
 * Executor configuration file loader.
 */
public final class ExecutorSettingsLoader {
  private static final Logger LOG = Logger.getLogger(ExecutorSettingsLoader.class.getName());
  /**
   * No instances of this class should exist.
   */
  private ExecutorSettingsLoader()  {
  }

  /**
   * Private helper function to simplify code.
   */
  private static ExecutorSettings parseExecutorSetting(JsonObject jsonExecSetting) {

    String executorName = jsonExecSetting.getAsJsonPrimitive("executorName").getAsString();

    String thermosObserver = "thermos".equals(executorName)
        ? jsonExecSetting.getAsJsonPrimitive("thermosObserverRoot").getAsString()
        : "";
    String executorPath = jsonExecSetting.getAsJsonPrimitive("executorPath") == null
        ? ""
        : jsonExecSetting.getAsJsonPrimitive("executorPath").getAsString();

    Optional<String> executorFlags = Optional
        .<String>fromNullable(jsonExecSetting.getAsJsonPrimitive("executorFlags").getAsString());

    JsonObject executorOverhead = jsonExecSetting.getAsJsonObject("executorOverhead");

    //TODO(rdelvalle): Check for nonsense values
    double numCpus = executorOverhead.getAsJsonPrimitive("numCpus").getAsDouble();
    long diskMB = executorOverhead.getAsJsonPrimitive("disk_mb").getAsLong();
    long ramMB = executorOverhead.getAsJsonPrimitive("ram_mb").getAsLong();
    int numPorts = executorOverhead.getAsJsonPrimitive("numPorts").getAsInt();

    Resources executorOverheadResources = new Resources(numCpus,
        Amount.of(ramMB, Data.MB),
        Amount.of(diskMB, Data.MB),
        numPorts);

    JsonArray executorResourses = jsonExecSetting.getAsJsonArray("executorResources");
    List<String> executorResourcesList = new ArrayList<String>();
    for (JsonElement resource: executorResourses) {
      executorResourcesList.add(resource.getAsJsonPrimitive().getAsString());
    }

    JsonArray executorGlobalContainerMounts = jsonExecSetting
        .getAsJsonArray("globalContainerMounts");
    List<Volume> globalMountsList = new ArrayList<Volume>();
    VolumeParser volParser = new VolumeParser();
    for (JsonElement mount: executorGlobalContainerMounts) {
      try {
        globalMountsList.add(volParser.doParse(mount.getAsString()));

      } catch (IllegalArgumentException e) {
        LOG.warning("Illegal global_mount setting: \"" + mount + "\" for " + executorName);
      }
    }

    return ExecutorSettings.newBuilder()
            .setExecutorName(executorName)
            .setExecutorPath(executorPath)
            .setExecutorResources(ImmutableList.<String>copyOf(executorResourcesList))
            .setThermosObserverRoot(thermosObserver)
            .setExecutorFlags(executorFlags)
            .setGlobalContainerMounts(ImmutableList.<Volume>copyOf(globalMountsList))
            .setExecutorOverhead(executorOverheadResources)
            .build();
  }

  public static ImmutableMap<String, ExecutorSettings> load(String configFilePath)
      throws ExecutorSettingsConfigException {

    Map<String, ExecutorSettings> executorSettings = new HashMap<String, ExecutorSettings>();

    try {
      Reader fileReader = new InputStreamReader(new FileInputStream(configFilePath), "UTF8");

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(fileReader);
      JsonArray executors = element.getAsJsonObject().getAsJsonArray("executors");

      for (JsonElement executor : executors) {
        ExecutorSettings temp = parseExecutorSetting(executor.getAsJsonObject());
        executorSettings.put(temp.getExecutorName(), temp);
      }

      return ImmutableMap.<String, ExecutorSettings>copyOf(executorSettings);

    } catch (FileNotFoundException e) {
      throw new ExecutorSettingsConfigException("Config file not found", e);
    } catch (UnsupportedEncodingException e) {
      throw new ExecutorSettingsConfigException("Config file needs to be in UTF8 format", e);
    } catch (JsonParseException e) {
      throw new ExecutorSettingsConfigException("Error parsing JSON", e);
    }
  }

  public static class ExecutorSettingsConfigException extends Exception {
    public ExecutorSettingsConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
