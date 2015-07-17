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

import java.util.Set;

import com.google.gson.JsonObject;

/**
 * Class to make serializing and deserializing executor configuration easier.
 */
public class ExecutorConfiguration {

  private String name;
  private String path;
  private String arguments;
  private Set<String> resources;
  private String thermosObserverRoot;
  private String executorFlags;
  private Set<String> globalContainerMounts;
  private ExecutorOverhead overhead = new ExecutorOverhead();
  private JsonObject customSchema;

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }
  public String getArguments() {
    return arguments;
  }

  public Set<String> getResources() {
    return resources;
  }

  public String getThermosObserverRoot() {
    return thermosObserverRoot;
  }

  public String getExecutorFlags() {
    return executorFlags;
  }

  public Set<String> getGlobalContainerMounts() {
    return globalContainerMounts;
  }

  public ExecutorOverhead getOverhead() {
    return overhead;
  }

  /**
   * Inner class to leverage GSON object parsing.
   */
  final static class ExecutorOverhead {
    private double numCpus;
    private long diskMB;
    private long ramMB;
    private int numPorts;

    private ExecutorOverhead() {
    }

    public double getNumCpus() {
      return numCpus;
    }

    public long getDiskMB() {
      return diskMB;
    }

    public long getRamMB() {
      return ramMB;
    }

    public int getNumPorts() {
      return numPorts;
    }
  }

  ExecutorConfiguration() {
  }
}
