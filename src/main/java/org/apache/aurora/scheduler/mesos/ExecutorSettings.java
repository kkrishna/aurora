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

import java.util.List;
import java.util.Objects;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.mesos.Protos.CommandInfo.URI;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for the executor to run, and resource overhead required for it.
 */
public final class ExecutorSettings {
  @SerializedName("name") private final String executorName;
  @SerializedName("command") private final List<String> executorCommand;
  @SerializedName("resources") private final List<URI> executorResources;
  @SerializedName("overhead") private final ResourceSlot executorOverhead;
  private final List<Volume> globalContainerMounts;
  private final String thermosObserverRoot;

  ExecutorSettings(
      String executorName,
      List<String> executorCommand,
      List<URI> executorResources,
      String thermosObserverRoot,
      ResourceSlot executorOverhead,
      List<Volume> globalContainerMounts) {

    this.executorName = executorName;
    this.executorCommand = requireNonNull(executorCommand);
    this.executorResources = requireNonNull(executorResources);
    this.thermosObserverRoot = requireNonNull(thermosObserverRoot);
    this.executorOverhead = requireNonNull(executorOverhead);
    this.globalContainerMounts = requireNonNull(globalContainerMounts);
  }

  public String getExecutorName() {
    return executorName;
  }

  public List<String> getExecutorCommand() {
    return executorCommand;
  }

  public List<URI> getExecutorResources() {
    return executorResources;
  }

  public String getThermosObserverRoot() {
    return thermosObserverRoot;
  }


  public ResourceSlot getExecutorOverhead() {
    return executorOverhead;
  }

  public List<Volume> getGlobalContainerMounts() {
    return globalContainerMounts;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        executorName,
        executorCommand,
        executorResources,
        thermosObserverRoot,
        executorCommand,
        executorOverhead,
        globalContainerMounts);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    final ExecutorSettings that = (ExecutorSettings) obj;

    return Objects.equals(executorName, that.executorName)
        && Objects.equals(executorCommand, that.executorCommand)
        && Objects.equals(executorResources, that.executorResources)
        && Objects.equals(thermosObserverRoot, that.thermosObserverRoot)
        && Objects.equals(executorOverhead, that.executorOverhead)
        && Objects.equals(globalContainerMounts, that.globalContainerMounts);
  }

  public static final class Builder {
    private String executorName;
    private List<String> executorCommand;
    private List<URI> executorResources;
    private String thermosObserverRoot;
    private ResourceSlot executorOverhead;
    private List<Volume> globalContainerMounts;

    Builder() {
      executorResources = ImmutableList.of();
      executorOverhead = ResourceSlot.NONE;
      globalContainerMounts = ImmutableList.of();
    }

    public Builder setExecutorName(String executorName) {
      this.executorName = executorName;
      return this;
    }

    public Builder setExecutorCommand(List<String> executorCommand) {
      this.executorCommand = executorCommand;
      return this;
    }

    public Builder setExecutorResources(List<URI> executorResources) {
      this.executorResources = executorResources;
      return this;
    }

    public Builder setThermosObserverRoot(String thermosObserverRoot) {
      this.thermosObserverRoot = thermosObserverRoot;
      return this;
    }

    public Builder setExecutorOverhead(ResourceSlot executorOverhead) {
      this.executorOverhead = executorOverhead;
      return this;
    }

    public Builder setGlobalContainerMounts(List<Volume> globalContainerMounts) {
      this.globalContainerMounts = globalContainerMounts;
      return this;
    }

    public ExecutorSettings build() {
      return new ExecutorSettings(
          executorName,
          executorCommand,
          executorResources,
          thermosObserverRoot,
          executorOverhead,
          globalContainerMounts);
    }
  }
}
