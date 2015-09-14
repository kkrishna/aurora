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

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.app.VolumeParser;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.*;

import static java.util.Objects.requireNonNull;

@JsonAutoDetect(fieldVisibility= JsonAutoDetect.Visibility.ANY)
public class ExecutorConfiguration {

  private String name;
  private List<String> command;
  private List<Resource> resources;
  private Overhead overhead;
  private List<String> globalContainerMounts;
  private Map<String,String> config;


  static final class Resource {
    private String value;
    private Optional<Boolean> executable;
    private Optional<Boolean> extract;
    private Optional<Boolean> cache;

    @JsonCreator
    Resource(@JsonProperty("value") String value) {
      this.value = requireNonNull(value);
      executable = Optional.empty();
      extract = Optional.empty();
      cache = Optional.empty();
    }

    public String getValue() {
      return value;
    }

    public Optional<Boolean> getExecutable() {
      return executable;
    }

    public Optional<Boolean> getExtract() {
      return extract;
    }

    public Optional<Boolean> getCache() {
      return cache;
    }

    public void setExecutable(Boolean executable) {
      this.executable = Optional.<Boolean>of(executable);
    }

    public void setExtract(Boolean extract) {
      this.extract = Optional.<Boolean>of(extract);
    }

    public void setCache(Boolean cache) {
      this.cache = Optional.<Boolean>of(cache);
    }
  }

  @JsonAutoDetect(fieldVisibility= JsonAutoDetect.Visibility.ANY)
  static final class Overhead {
    private double numCpus;
    private long diskMB;
    private long ramMB;
    private int numPorts;

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

  public List<String> getCommand() {
    return command;
  }

  public List<Resource> getResources() {
    return resources;
  }

  public Overhead getOverhead() {
    return overhead;
  }

  public List<String> getGlobalContainerMounts() {
    return globalContainerMounts;
  }

  public Map<String, String> getConfig() {
    return config;
  }
}
