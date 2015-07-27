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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;

/**
 * Class to make serializing and de-serializing executor configuration easier.
 */
public class ExecutorConfiguration {
  private static final Logger LOG = Logger.getLogger(ExecutorConfiguration.class.getName());

  private String name;
  private String path;
  private Set<String> resources;
  private String thermosObserverRoot;
  private String executorFlags;
  private Set<String> globalContainerMounts;
  private ExecutorOverhead overhead;
  private JsonObject customSchema;

  /**
   * Inner class to encapsulate Overhead into a JSON object.
   */
  private static final class ExecutorOverhead {
    private double numCpus;
    private long ramMB;
    private long diskMB;
    private int numPorts;
  }

  public String getName() {
    return name;
  }

  public JsonObject getCustomSchema() {
    return customSchema;
  }

  /**
   * Helper method to convert information from config file into a Volume list. Uses
   * VolumeParser class to verify correctness.
   */
  private static ImmutableList<Volume> globalContainerMountParser(
      Set<String> globalContainerMounts) {

    List<Volume> globalMountsList = new ArrayList<Volume>();
    VolumeParser volParser = new VolumeParser();

    for (String mount : globalContainerMounts) {
      try {
        globalMountsList.add(volParser.doParse(mount));
      } catch (IllegalArgumentException e) {
        LOG.warning("Illegal global container mount setting \"" + mount + "\" is being ignored");
      }
    }

    return ImmutableList.<Volume>copyOf(globalMountsList);
  }

  /**
   * Method to convert ExecutorConfiguration to ExecutorSettings used by Aurora Scheduler.
   */
  public ExecutorSettings toExecutorSettings() {
    return ExecutorSettings.newBuilder()
        .setExecutorName(this.name)
        .setExecutorPath(this.path)
        .setExecutorFlags(Optional.<String>fromNullable(this.executorFlags))
        .setGlobalContainerMounts(globalContainerMountParser(this.globalContainerMounts))
        .setExecutorResources(ImmutableList.<String>copyOf(this.resources))
        .setThermosObserverRoot(this.thermosObserverRoot)
        .setExecutorOverhead(
            new Resources(overhead.numCpus,
                Amount.of(overhead.ramMB, Data.MB),
                Amount.of(overhead.diskMB, Data.MB),
                overhead.numPorts)
        )
        .build();
  }

}
