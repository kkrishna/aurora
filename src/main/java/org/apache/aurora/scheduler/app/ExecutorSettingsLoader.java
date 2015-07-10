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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.google.gson.*;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;

/**
 * Executor configuration file loader
 */
public class ExecutorSettingsLoader {
  private static final Logger LOG = Logger.getLogger(ExecutorSettingsLoader.class.getName());
  /**
   * No instances of this class should exist
   */
  private ExecutorSettingsLoader(){
  }

  /**
   * Private helper function to simplify code
   */
  private static ExecutorSettings parseExecutorSetting(JsonObject jsonExecSetting){

    String executorName = jsonExecSetting.getAsJsonPrimitive("executorName").getAsString();

    if(!executorName.equals("mesos-command")) {
      //TODO(rdelvalle): Require more stuff if not mesos
    }

    String executorPath = (jsonExecSetting.getAsJsonPrimitive("executorPath") != null)
        ? jsonExecSetting.getAsJsonPrimitive("executorPath").getAsString()
        : "";
    String thermosObserver = (jsonExecSetting.getAsJsonPrimitive("thermosObserverRoot") != null)
        ? jsonExecSetting.getAsJsonPrimitive("thermosObserverRoot").getAsString()
        : "";
    Optional<String> executorFlags = Optional
        .<String>fromNullable(jsonExecSetting.getAsJsonPrimitive("executorFlags").getAsString());
    String executorCommandValue = (jsonExecSetting.getAsJsonPrimitive("commandValue") != null)
        ? jsonExecSetting.getAsJsonPrimitive("commandValue").getAsString()
        : "";

    JsonObject executorOverhead = jsonExecSetting.getAsJsonObject("executorOverhead");

    //TODO(rdelvalle): Check for nonsense values
    double numCpus = executorOverhead.getAsJsonPrimitive("numCpus").getAsDouble();
    long disk_mb = executorOverhead.getAsJsonPrimitive("disk_mb").getAsLong();
    long ram_mb = executorOverhead.getAsJsonPrimitive("ram_mb").getAsLong();
    int numPorts = executorOverhead.getAsJsonPrimitive("numPorts").getAsInt();

    Resources executor_overhead_resources = new Resources(numCpus,
        Amount.of(ram_mb, Data.MB),
        Amount.of(disk_mb, Data.MB),
        numPorts);

    JsonArray executorResourses = jsonExecSetting.getAsJsonArray("executorResources");
    List<String> executorResourcesList = new ArrayList<String>();
    for(JsonElement resource: executorResourses) {
      executorResourcesList.add(resource.getAsJsonPrimitive().getAsString());
    }

    JsonArray executorGlobalContainerMounts = jsonExecSetting.getAsJsonArray("globalContainerMounts");
    List<Volume> global_mounts_list = new ArrayList<Volume>();
    VolumeParser volParser = new VolumeParser();
    for(JsonElement mount: executorGlobalContainerMounts) {
      try {
      global_mounts_list.add(volParser.doParse(mount.getAsString()));

      } catch(IllegalArgumentException e)
      {
        LOG.warning("Illegal global_mount setting: \"" + mount + "\" for " + executorName);
      }
    }

    return ExecutorSettings.newBuilder()
            .setExecutorName(executorName)
            .setExecutorPath(executorPath)
            .setExecutorResources(ImmutableList.<String>copyOf(executorResourcesList))
            .setThermosObserverRoot(thermosObserver)
            .setExecutorFlags(executorFlags)
            .setGlobalContainerMounts(ImmutableList.<Volume>copyOf(global_mounts_list))
            .setExecutorOverhead(executor_overhead_resources)
            .build();
  }

  public static ImmutableMap<String, ExecutorSettings> load(String configFilePath)
      throws FileNotFoundException {

    Map<String, ExecutorSettings> executorSettings = new HashMap<String, ExecutorSettings>();

    try {
      BufferedReader fileReader = new BufferedReader(new FileReader(configFilePath));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(fileReader);
      JsonArray executors = element.getAsJsonObject().getAsJsonArray("executors");

      if(executors.isJsonArray()) {
        for(JsonElement executor: executors) {
          ExecutorSettings temp = parseExecutorSetting(executor.getAsJsonObject());
          executorSettings.put(temp.getExecutorName(), temp);
        }
      } else {
        //TODO(rdelvalle): Throw error for malformed config file
      }

      return ImmutableMap.<String, ExecutorSettings>copyOf(executorSettings);

    } catch(FileNotFoundException e) {
      throw e;

    }
  }

}
